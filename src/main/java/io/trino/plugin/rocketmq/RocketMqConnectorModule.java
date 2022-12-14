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
package io.trino.plugin.rocketmq;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorMetadata;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSinkProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorRecordSetProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitManager;
import io.trino.plugin.base.classloader.ForClassLoaderSafe;
import io.trino.plugin.rocketmq.record.RocketMqRecordSetProvider;
import io.trino.plugin.rocketmq.schema.RocketMqTopicDescription;
import io.trino.plugin.rocketmq.schema.file.FileTableDescriptionSupplier;
import io.trino.plugin.rocketmq.schema.file.FileTableDescriptionSupplierModule;
import io.trino.plugin.rocketmq.split.RocketMqSplitManager;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSplitManager;

import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

public class RocketMqConnectorModule
        extends AbstractConfigurationAwareModule {
    @Override
    public void setup(Binder binder) {

        // Bind rocketmq config
        configBinder(binder).bindConfig(RocketMqConfig.class);

        // Bind connector metadata
        binder.bind(ConnectorMetadata.class).annotatedWith(ForClassLoaderSafe.class).to(RocketMqMetadata.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorMetadata.class).to(ClassLoaderSafeConnectorMetadata.class).in(Scopes.SINGLETON);

        // Bind connector split manager, Split data into multiple blocks
        binder.bind(ConnectorSplitManager.class).annotatedWith(ForClassLoaderSafe.class).to(RocketMqSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorSplitManager.class).to(ClassLoaderSafeConnectorSplitManager.class).in(Scopes.SINGLETON);

        // Bind connector record set provider
        binder.bind(ConnectorRecordSetProvider.class).annotatedWith(ForClassLoaderSafe.class).to(RocketMqRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorRecordSetProvider.class).to(ClassLoaderSafeConnectorRecordSetProvider.class).in(Scopes.SINGLETON);

        // Bind page sink provider
        binder.bind(ConnectorPageSinkProvider.class).annotatedWith(ForClassLoaderSafe.class).to(RocketMqPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSinkProvider.class).to(ClassLoaderSafeConnectorPageSinkProvider.class).in(Scopes.SINGLETON);

        // Bind rocketmq connector
        binder.bind(RocketMqConnector.class).in(Scopes.SINGLETON);

        // Bind rocketmq internal filed manager
        binder.bind(RocketMqInternalFieldManager.class).in(Scopes.SINGLETON);
        binder.bind(RocketMqFilterManager.class).in(Scopes.SINGLETON);

        // Bind file table description supplier
        bindTopicSchemaProviderModule(FileTableDescriptionSupplier.NAME, new FileTableDescriptionSupplierModule());

        jsonCodecBinder(binder).bindJsonCodec(RocketMqTopicDescription.class);
    }

    public void bindTopicSchemaProviderModule(String name, Module module) {
        install(conditionalModule(
                RocketMqConfig.class,
                rocketMQConfig -> name.equalsIgnoreCase(rocketMQConfig.getTableDescriptionSupplier()),
                module));
    }
}
