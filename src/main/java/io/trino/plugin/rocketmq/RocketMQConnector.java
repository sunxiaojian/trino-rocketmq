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

import io.airlift.bootstrap.LifeCycleManager;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.session.PropertyMetadata;

import javax.inject.Inject;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;

/**
 * RocketMQ connector
 */
public class RocketMQConnector implements Connector {

    private final LifeCycleManager lifeCycleManager;
    private final ConnectorMetadata metadata;
    private final ConnectorSplitManager splitManager;
    private final ConnectorPageSinkProvider pageSinkProvider;

    private final List<PropertyMetadata<?>> sessionProperties;
    private final  ConnectorRecordSetProvider recordSetProvider;
    @Inject
    public RocketMQConnector(
            LifeCycleManager lifeCycleManager,
            ConnectorMetadata connectorMetadata,
            ConnectorSplitManager connectorSplitManager,
            ConnectorRecordSetProvider recordSetProvider,
            ConnectorPageSinkProvider pageSinkProvider,
            Set<SessionPropertiesProvider> sessionProperties
    ){
        this.lifeCycleManager = lifeCycleManager;
        this.metadata = connectorMetadata;
        this.splitManager = connectorSplitManager;
        this.pageSinkProvider = pageSinkProvider;
        this.sessionProperties = sessionProperties.stream()
                .flatMap(sessionPropertiesProvider -> sessionPropertiesProvider.getSessionProperties().stream())
                .collect(toImmutableList());
        this.recordSetProvider = recordSetProvider;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle) {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return splitManager;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider() {
        return recordSetProvider;
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider() {
        return pageSinkProvider;
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties() {
        return sessionProperties;
    }

    @Override
    public final void shutdown() {
        lifeCycleManager.stop();
    }
}
