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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorMetadata;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSinkProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorRecordSetProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitManager;
import io.trino.plugin.base.classloader.ForClassLoaderSafe;
import io.trino.plugin.rocketmq.record.RocketMQRecordSetProvider;
import io.trino.plugin.rocketmq.schema.RocketMQTopicDescription;
import io.trino.plugin.rocketmq.schema.file.FileTableDescriptionSupplier;
import io.trino.plugin.rocketmq.schema.file.FileTableDescriptionSupplierModule;
import io.trino.plugin.rocketmq.split.RocketMQSplitManager;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSplitManager;

import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

public class RocketMQConnectorModule
        extends AbstractConfigurationAwareModule
{
    @Override
    public void setup(Binder binder)
    {
        binder.bind(ConnectorMetadata.class).annotatedWith(ForClassLoaderSafe.class).to(RocketMQMetadata.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorMetadata.class).to(ClassLoaderSafeConnectorMetadata.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorSplitManager.class).annotatedWith(ForClassLoaderSafe.class).to(RocketMQSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorSplitManager.class).to(ClassLoaderSafeConnectorSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorRecordSetProvider.class).annotatedWith(ForClassLoaderSafe.class).to(RocketMQRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorRecordSetProvider.class).to(ClassLoaderSafeConnectorRecordSetProvider.class).in(Scopes.SINGLETON);
       //  binder.bind(ConnectorPageSinkProvider.class).annotatedWith(ForClassLoaderSafe.class).to(RocketMQPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSinkProvider.class).to(ClassLoaderSafeConnectorPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(RocketMQConnector.class).in(Scopes.SINGLETON);
        binder.bind(RocketMQInternalFieldManager.class).in(Scopes.SINGLETON);


        configBinder(binder).bindConfig(RocketMQConfig.class);
        //
        bindTopicSchemaProviderModule(FileTableDescriptionSupplier.NAME, new FileTableDescriptionSupplierModule());
//         bindTopicSchemaProviderModule(ConfluentSchemaRegistryTableDescriptionSupplier.NAME, new ConfluentModule());
//        newSetBinder(binder, SessionPropertiesProvider.class).addBinding().to(KafkaSessionProperties.class).in(Scopes.SINGLETON);
        jsonCodecBinder(binder).bindJsonCodec(RocketMQTopicDescription.class);
    }

    public void bindTopicSchemaProviderModule(String name, Module module) {
        install(conditionalModule(
                RocketMQConfig.class,
                rocketMQConfig -> name.equalsIgnoreCase(rocketMQConfig.getTableDescriptionSupplier()),
                module));
    }
}
