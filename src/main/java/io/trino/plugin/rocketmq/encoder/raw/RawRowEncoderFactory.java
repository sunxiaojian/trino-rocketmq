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
package io.trino.plugin.rocketmq.encoder.raw;

import io.trino.plugin.rocketmq.encoder.EncoderColumnHandle;
import io.trino.plugin.rocketmq.encoder.RowEncoder;
import io.trino.plugin.rocketmq.encoder.RowEncoderFactory;
import io.trino.spi.connector.ConnectorSession;

import java.util.List;
import java.util.Optional;

public class RawRowEncoderFactory implements RowEncoderFactory {
    @Override
    public RowEncoder create(ConnectorSession session, Optional<String> dataSchema, List<EncoderColumnHandle> columnHandles) {
        return new RawRowEncoder(session, columnHandles);
    }
}
