/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.rocketmq.record;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import io.airlift.slice.Slice;
import io.trino.decoder.DecoderColumnHandle;
import io.trino.decoder.FieldValueProvider;
import io.trino.decoder.RowDecoder;
import io.trino.plugin.rocketmq.RocketMQColumnHandle;
import io.trino.plugin.rocketmq.RocketMQConsumerFactory;
import io.trino.plugin.rocketmq.split.RocketMQSplit;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.decoder.FieldValueProviders.bytesValueProvider;
import static io.trino.decoder.FieldValueProviders.longValueProvider;
import static io.trino.plugin.rocketmq.RocketMQInternalFieldManager.KEY_FIELD;
import static io.trino.plugin.rocketmq.RocketMQInternalFieldManager.KEY_LENGTH_FIELD;
import static io.trino.plugin.rocketmq.RocketMQInternalFieldManager.MESSAGE_FIELD;
import static io.trino.plugin.rocketmq.RocketMQInternalFieldManager.MESSAGE_LENGTH_FIELD;
import static io.trino.plugin.rocketmq.RocketMQInternalFieldManager.MESSAGE_PROPERTIES;
import static io.trino.plugin.rocketmq.RocketMQInternalFieldManager.MSG_ID;
import static io.trino.plugin.rocketmq.RocketMQInternalFieldManager.OFFSET_TIMESTAMP_FIELD;
import static io.trino.plugin.rocketmq.RocketMQInternalFieldManager.QUEUE_ID_FIELD;
import static io.trino.plugin.rocketmq.RocketMQInternalFieldManager.QUEUE_OFFSET_FIELD;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static java.lang.Math.max;
import static java.util.Collections.emptyIterator;
import static java.util.Objects.requireNonNull;

/**
 * RocketMQ record cursor
 */
public class RocketMQRecordCursor implements RecordCursor {

    private static final int CONSUMER_POLL_TIMEOUT = 3000;
    private final RocketMQSplit split;
    private final MessageQueue messageQueue;
    private final DefaultLitePullConsumer defaultLitePullConsumer;
    private final List<RocketMQColumnHandle> columnHandles;

    private Iterator<MessageExt> records = emptyIterator();

    private final RowDecoder keyDecoder;
    private final RowDecoder messageDecoder;

    private final FieldValueProvider[] currentRowValues;

    // calc all bytes
    private long completedBytes;

    public RocketMQRecordCursor(
            RocketMQSplit split,
            RocketMQConsumerFactory consumerFactory,
            List<RocketMQColumnHandle> columnHandles,
            ConnectorSession connectorSession,
            RowDecoder keyDecoder,
            RowDecoder messageDecoder

    ){
        this.split = requireNonNull(split, "split is null");
        this.messageQueue = new MessageQueue(split.getTopicName(), split.getBrokerName(), split.getQueueId());
        requireNonNull(consumerFactory, "consumerFactory is null");

        defaultLitePullConsumer = consumerFactory.create(connectorSession);
        defaultLitePullConsumer.setConsumerGroup(UUID.randomUUID().toString());
        defaultLitePullConsumer.assign(ImmutableList.of(this.messageQueue));
        try {
            // consumer seek
            defaultLitePullConsumer.seek(this.messageQueue, split.getMessagesRange().getBegin());
        } catch (MQClientException e) {
            throw new RuntimeException(e);
        }

        this.columnHandles = requireNonNull(columnHandles, "columnHandles null");
        this.currentRowValues = new FieldValueProvider[columnHandles.size()];
        // key decoder
        this.keyDecoder = keyDecoder;
        // message decoder
        this.messageDecoder = messageDecoder;
    }

    @Override
    public long getCompletedBytes() {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    @Override
    public Type getType(int field) {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getType();
    }

    // read record
    @Override
    public boolean advanceNextPosition() {
        if (!records.hasNext()) {
            long currentOffset = defaultLitePullConsumer.getOffsetStore().readOffset(messageQueue, ReadOffsetType.READ_FROM_MEMORY);
            if (currentOffset >= split.getMessagesRange().getEnd()) {
                return false;
            }
            records = defaultLitePullConsumer.poll(CONSUMER_POLL_TIMEOUT).iterator();
            return advanceNextPosition();
        }
        return nextRow(records.next());
    }

    private boolean nextRow(MessageExt message) {
        requireNonNull(message, "message is null");
        if (message.getQueueOffset() >= split.getMessagesRange().getEnd()) {
            return false;
        }
        // completed bytes
        completedBytes += max(message.getStoreSize(), 0);

        byte[] keyData = new byte[0];
        if (message.getKeys() != null) {
            try {
                keyData = message.getKeys().getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }

        byte[] messageData = new byte[0];
        if (message.getBody() != null) {
            messageData = message.getBody();
        }
        long timeStamp = message.getBornTimestamp();

        Map<ColumnHandle, FieldValueProvider> currentRowValuesMap = new HashMap<>();

        Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodedKey = keyDecoder.decodeRow(keyData);
        Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodedValue = messageDecoder.decodeRow(messageData);

        for (DecoderColumnHandle columnHandle : columnHandles) {
            if (columnHandle.isInternal()) {
                switch (columnHandle.getName()) {
                    case QUEUE_ID_FIELD:
                        currentRowValuesMap.put(columnHandle, longValueProvider(message.getQueueId()));
                        break;
                    case QUEUE_OFFSET_FIELD:
                        currentRowValuesMap.put(columnHandle, longValueProvider(message.getQueueOffset()));
                        break;
                    case MESSAGE_FIELD:
                        currentRowValuesMap.put(columnHandle, bytesValueProvider(messageData));
                        break;
                    case MESSAGE_LENGTH_FIELD:
                        currentRowValuesMap.put(columnHandle, longValueProvider(messageData.length));
                        break;
                    case KEY_FIELD:
                        currentRowValuesMap.put(columnHandle, bytesValueProvider(keyData));
                        break;
                    case KEY_LENGTH_FIELD:
                        currentRowValuesMap.put(columnHandle, longValueProvider(keyData.length));
                        break;
                    case OFFSET_TIMESTAMP_FIELD:
                        timeStamp *= MICROSECONDS_PER_MILLISECOND;
                        currentRowValuesMap.put(columnHandle, longValueProvider(timeStamp));
                        break;
                    case MESSAGE_PROPERTIES:
                        currentRowValuesMap.put(columnHandle, propertiesValueProvider((MapType) columnHandle.getType(), message.getProperties()));
                        break;
                    case MSG_ID:
                        currentRowValuesMap.put(columnHandle, bytesValueProvider(message.getMsgId().getBytes()));
                        break;
                    default:
                        throw new IllegalArgumentException("unknown internal field " + columnHandle.getName());
                }
            }
        }

        decodedKey.ifPresent(currentRowValuesMap::putAll);
        decodedValue.ifPresent(currentRowValuesMap::putAll);

        for (int i = 0; i < columnHandles.size(); i++) {
            ColumnHandle columnHandle = columnHandles.get(i);
            currentRowValues[i] = currentRowValuesMap.get(columnHandle);
        }
        return true; // Advanced successfully.
    }



    public static FieldValueProvider propertiesValueProvider(MapType varcharMapType, Map<String, String> props)
    {
        Type keyType = varcharMapType.getTypeParameters().get(0);
        Type valueArrayType = varcharMapType.getTypeParameters().get(1);
        Type valueType = valueArrayType.getTypeParameters().get(0);

        BlockBuilder mapBlockBuilder = varcharMapType.createBlockBuilder(null, 1);
        BlockBuilder builder = mapBlockBuilder.beginBlockEntry();

        // Group by keys and collect values as array.
        Multimap<String, String> headerMap = ArrayListMultimap.create();
        for (Map.Entry<String, String> propEntry : props.entrySet()) {
            headerMap.put(propEntry.getKey(), propEntry.getValue());
        }
        for (String headerKey : headerMap.keySet()) {
            writeNativeValue(keyType, builder, headerKey);
            BlockBuilder arrayBuilder = builder.beginBlockEntry();
            for (String value : headerMap.get(headerKey)) {
                writeNativeValue(valueType, arrayBuilder, value);
            }
            builder.closeEntry();
        }
        mapBlockBuilder.closeEntry();
        return new FieldValueProvider() {
            @Override
            public boolean isNull()
            {
                return false;
            }
            @Override
            public Block getBlock()
            {
                return varcharMapType.getObject(mapBlockBuilder, 0);
            }
        };
    }


    @Override
    public boolean getBoolean(int field) {
        return getFieldValueProvider(field, boolean.class).getBoolean();
    }

    @Override
    public long getLong(int field) {
        return getFieldValueProvider(field, long.class).getLong();
    }

    @Override
    public double getDouble(int field) {
        return getFieldValueProvider(field, double.class).getDouble();
    }

    @Override
    public Slice getSlice(int field) {
        return getFieldValueProvider(field, Slice.class).getSlice();
    }

    @Override
    public Object getObject(int field) {
        return getFieldValueProvider(field, Block.class).getBlock();
    }

    @Override
    public boolean isNull(int field) {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return currentRowValues[field] == null || currentRowValues[field].isNull();
    }




    @Override
    public void close() {
        this.defaultLitePullConsumer.shutdown();
    }


    private FieldValueProvider getFieldValueProvider(int field, Class<?> expectedType) {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        checkFieldType(field, expectedType);
        return currentRowValues[field];
    }

    private void checkFieldType(int field, Class<?> expected) {
        Class<?> actual = getType(field).getJavaType();
        checkArgument(actual == expected, "Expected field %s to be type %s but is %s", field, expected, actual);
    }
}