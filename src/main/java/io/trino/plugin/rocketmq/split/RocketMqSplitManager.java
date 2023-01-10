/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.trino.plugin.rocketmq.split;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.trino.plugin.rocketmq.RocketMqConfig;
import io.trino.plugin.rocketmq.RocketMqConsumerFactory;
import io.trino.plugin.rocketmq.RocketMqFilterManager;
import io.trino.plugin.rocketmq.RocketMqFilteringResult;
import io.trino.plugin.rocketmq.RocketMqTableHandle;
import io.trino.plugin.rocketmq.schema.ContentSchemaReader;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.admin.TopicOffset;
import org.apache.rocketmq.common.admin.TopicStatsTable;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * RocketMQ topic split manager
 */
public class RocketMqSplitManager implements ConnectorSplitManager {

    private final RocketMqConsumerFactory consumerFactory;
    private final ContentSchemaReader contentSchemaReader;
    private final int messagesPerSplit;
    private final RocketMqFilterManager filterManager;

    @Inject
    public RocketMqSplitManager(RocketMqConsumerFactory consumerFactory, RocketMqConfig config, ContentSchemaReader contentSchemaReader, RocketMqFilterManager filterManager) {
        this.consumerFactory = requireNonNull(consumerFactory, "consumerFactory is null");
        this.messagesPerSplit = config.getMessagesPerSplit();
        this.contentSchemaReader = requireNonNull(contentSchemaReader, "contentSchemaReader is null");
        this.filterManager = requireNonNull(filterManager, "filterManager is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint) {

        ImmutableList.Builder<RocketMqSplit> splits = ImmutableList.builder();
        RocketMqTableHandle tableHandle = (RocketMqTableHandle) table;
        DefaultMQAdminExt adminClient = consumerFactory.admin(session);
        try {

            TopicStatsTable topicStatsTable = adminClient.examineTopicStats(tableHandle.getTopicName());
            HashMap<MessageQueue, TopicOffset> offsets = topicStatsTable.getOffsetTable();
            // admin shutdown
            adminClient.shutdown();

            Optional<String> keyDataSchemaContents = contentSchemaReader.readKeyContentSchema(tableHandle);
            Optional<String> messageDataSchemaContents = contentSchemaReader.readValueContentSchema(tableHandle);

            RocketMqFilteringResult filterResult= filterManager.filter(session, tableHandle, Lists.newArrayList(offsets.keySet()),  offsets);
            for (Map.Entry<MessageQueue, TopicOffset> offset : filterResult.getMessageQueueTopicOffsets().entrySet()) {
                // build rocketmq split
                MessageQueue queue = offset.getKey();
                TopicOffset topicOffset = offset.getValue();
                List<Range> ranges = new Range(topicOffset.getMinOffset(), topicOffset.getMaxOffset())
                        .partition(messagesPerSplit);
                ranges.stream().map(range -> new RocketMqSplit(
                        tableHandle.getTopicName(),
                        tableHandle.getKeyDataFormat(),
                        tableHandle.getMessageDataFormat(),
                        keyDataSchemaContents,
                        messageDataSchemaContents,
                        queue.getQueueId(),
                        queue.getBrokerName(),
                        range,
                        consumerFactory.hostAddress()
                        )).forEach(splits::add);
            }

        } catch (RemotingException | MQClientException | InterruptedException | MQBrokerException e) {
            throw new RuntimeException(e);
        }
        return new FixedSplitSource(splits.build());
    }
}
