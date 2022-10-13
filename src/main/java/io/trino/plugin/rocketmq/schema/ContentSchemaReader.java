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
package io.trino.plugin.rocketmq.schema;

import io.trino.plugin.rocketmq.RocketMQTableHandle;

import java.util.Optional;

/**
 * content schema reader
 */
public interface ContentSchemaReader {
    /**
     * read key content schema
     * @param tableHandle
     * @return
     */
    Optional<String> readKeyContentSchema(RocketMQTableHandle tableHandle);

    /**
     * read value content schema
     * @param tableHandle
     * @return
     */
    Optional<String> readValueContentSchema(RocketMQTableHandle tableHandle);
}
