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

package io.trino.plugin.rocketmq.record;

import com.google.common.collect.ImmutableMap;
import io.trino.decoder.DispatchingRowDecoderFactory;
import io.trino.decoder.RowDecoder;
import io.trino.plugin.rocketmq.RocketMQColumnHandle;
import io.trino.plugin.rocketmq.RocketMQConsumerFactory;
import io.trino.plugin.rocketmq.split.RocketMQSplit;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.RecordSet;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

/**
 * rocketmq record set provider
 */
public class RocketMQRecordSetProvider implements ConnectorRecordSetProvider {

    private final DispatchingRowDecoderFactory decoderFactory;
    private final RocketMQConsumerFactory consumerFactory;

    @Inject
    public RocketMQRecordSetProvider(DispatchingRowDecoderFactory decoderFactory, RocketMQConsumerFactory consumerFactory) {
        this.decoderFactory = requireNonNull(decoderFactory, "decoderFactory is null");
        this.consumerFactory = requireNonNull(consumerFactory, "consumerFactory is null");
    }

    private static Map<String, String> getDecoderParameters(Optional<String> dataSchema) {
        ImmutableMap.Builder<String, String> parameters = ImmutableMap.builder();
        dataSchema.ifPresent(schema -> parameters.put("dataSchema", schema));
        return parameters.buildOrThrow();
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<? extends ColumnHandle> columns) {
        RocketMQSplit rocketMQSplit = (RocketMQSplit) split;

        List<RocketMQColumnHandle> rocketMQColumnHandles = columns.stream()
                .map(RocketMQColumnHandle.class::cast)
                .collect(toImmutableList());

        RowDecoder keyDecoder = decoderFactory.create(
                rocketMQSplit.getKeyDataFormat(),
                getDecoderParameters(rocketMQSplit.getKeyDataSchemaContents()),
                rocketMQColumnHandles.stream()
                        .filter(col -> !col.isInternal())
                        .filter(RocketMQColumnHandle::isKeyCodec)
                        .collect(toImmutableSet()));

        RowDecoder messageDecoder = decoderFactory.create(
                rocketMQSplit.getMessageDataFormat(),
                getDecoderParameters(rocketMQSplit.getMessageDataSchemaContents()),
                rocketMQColumnHandles.stream()
                        .filter(col -> !col.isInternal())
                        .filter(col -> !col.isKeyCodec())
                        .collect(toImmutableSet()));

        return new RocketMQRecordSet(rocketMQSplit, consumerFactory, rocketMQColumnHandles, session, keyDecoder, messageDecoder);
    }
}
