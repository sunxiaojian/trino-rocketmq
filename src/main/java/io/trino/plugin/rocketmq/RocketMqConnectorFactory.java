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

import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.trino.plugin.base.CatalogNameModule;
import io.trino.plugin.base.TypeDeserializerModule;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.type.TypeManager;

import java.util.Map;

import static io.trino.plugin.base.Versions.checkSpiVersion;
import static java.util.Objects.requireNonNull;


/**
 * RocketMQ connector factory
 */
public class RocketMqConnectorFactory implements ConnectorFactory {


    private final Module extension;

    public RocketMqConnectorFactory(Module extension) {
        this.extension = requireNonNull(extension, "extension is null");
    }


    public String getName() {
        return "rocketmq";
    }

    /**
     * create connector
     *
     * @param catalogName
     * @param config
     * @param context
     * @return
     */
    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
        requireNonNull(catalogName, "Catalog name is null");
        requireNonNull(config, "Config is null");
        checkSpiVersion(context, this);
        // registry module
        Bootstrap bootstrap = new Bootstrap(
                new CatalogNameModule(catalogName),
                new JsonModule(),
                new TypeDeserializerModule(context.getTypeManager()),
                new RocketMqConnectorModule(),
                extension,
                binder -> {
                    binder.bind(ClassLoader.class).toInstance(RocketMqConnectorFactory.class.getClassLoader());
                    binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                    binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                });
        // initialize
        Injector injector = bootstrap
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize();
        return injector.getInstance(RocketMqConnector.class);
    }
}
