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
package io.trino.plugin.rocketmq;

import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSink;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * RocketMQ page sink
 */
public class RocketMQPageSink implements ConnectorPageSink {
    @Override
    public CompletableFuture<?> appendPage(Page page) {
        return null;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish() {
        return null;
    }

    @Override
    public void abort() {

    }
}
