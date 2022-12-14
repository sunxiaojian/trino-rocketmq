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

import io.trino.decoder.RowDecoder;
import io.trino.plugin.rocketmq.handle.RocketMqColumnHandle;
import io.trino.plugin.rocketmq.client.RocketMqConsumerFactory;
import io.trino.plugin.rocketmq.split.RocketMqSplit;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;

/**
 * rocketmq record set
 */
public class RocketMqRecordSet implements RecordSet {
    private final RocketMqSplit split;
    private final RocketMqConsumerFactory consumerFactory;
    private final List<RocketMqColumnHandle> columnHandles;
    private final ConnectorSession connectorSession;
    private final RowDecoder keyDecoder;
    private final RowDecoder messageDecoder;
    private final List<Type> columnTypes;

    public RocketMqRecordSet(RocketMqSplit split,
                             RocketMqConsumerFactory consumerFactory,
                             List<RocketMqColumnHandle> columnHandles,
                             ConnectorSession connectorSession,
                             RowDecoder keyDecoder,
                             RowDecoder messageDecoder) {
        this.split = split;
        this.consumerFactory = consumerFactory;
        this.columnHandles = columnHandles;
        this.connectorSession = connectorSession;
        this.keyDecoder = keyDecoder;
        this.messageDecoder = messageDecoder;
        this.columnTypes = columnHandles.stream()
                .map(RocketMqColumnHandle::getType)
                .collect(toImmutableList());

    }

    @Override
    public List<Type> getColumnTypes() {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor() {
        return new RocketMqRecordCursor(
                split,
                consumerFactory,
                columnHandles,
                connectorSession,
                keyDecoder,
                messageDecoder
        );
    }
}
