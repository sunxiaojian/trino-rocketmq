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


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.trino.decoder.dummy.DummyRowDecoder;
import io.trino.plugin.rocketmq.schema.RocketMQTopicDescription;
import io.trino.plugin.rocketmq.schema.RocketMQTopicFieldDescription;
import io.trino.plugin.rocketmq.schema.RocketMQTopicFieldGroup;
import io.trino.plugin.rocketmq.schema.TableDescriptionSupplier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ComputedStatistics;

import javax.inject.Inject;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static java.util.Objects.requireNonNull;

/**
 * rocketmq metadata
 */
public class RocketMQMetadata implements ConnectorMetadata {
    private final boolean hideInternalColumns;
    private final RocketMQInternalFieldManager rocketMQInternalFieldManager;
    private final TableDescriptionSupplier tableDescriptionSupplier;

    @Inject
    public RocketMQMetadata(RocketMQConfig config,
                            RocketMQInternalFieldManager rocketMQInternalFieldManager,
                            TableDescriptionSupplier tableDescriptionSupplier) {
        this.hideInternalColumns = config.isHideInternalColumns();
        this.rocketMQInternalFieldManager = rocketMQInternalFieldManager;
        this.tableDescriptionSupplier = tableDescriptionSupplier;
    }

    private static String getDataFormat(Optional<RocketMQTopicFieldGroup> fieldGroup) {
        return fieldGroup.map(RocketMQTopicFieldGroup::getDataFormat).orElse(DummyRowDecoder.NAME);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return tableDescriptionSupplier.listTables().stream()
                .map(SchemaTableName::getSchemaName)
                .collect(toImmutableList());
    }

    @Override
    public RocketMQTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName) {
        return getTopicDescription(session, schemaTableName)
                .map(rocketMQTopicDescription -> new RocketMQTableHandle(
                        schemaTableName.getSchemaName(),
                        schemaTableName.getTableName(),
                        rocketMQTopicDescription.getTopicName(),
                        getDataFormat(rocketMQTopicDescription.getKey()),
                        getDataFormat(rocketMQTopicDescription.getMessage()),
                        rocketMQTopicDescription.getKey().flatMap(RocketMQTopicFieldGroup::getDataSchema),
                        rocketMQTopicDescription.getMessage().flatMap(RocketMQTopicFieldGroup::getDataSchema),
                        rocketMQTopicDescription.getKey().flatMap(RocketMQTopicFieldGroup::getSubject),
                        rocketMQTopicDescription.getMessage().flatMap(RocketMQTopicFieldGroup::getSubject),
                        getColumnHandles(session, schemaTableName).values().stream()
                                .map(RocketMQColumnHandle.class::cast)
                                .collect(toImmutableList()),
                        TupleDomain.all()))
                .orElse(null);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle) {
        return getTableMetadata(session, ((RocketMQTableHandle) tableHandle).toSchemaTableName());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
        return tableDescriptionSupplier.listTables().stream()
                .filter(tableName -> schemaName.map(tableName.getSchemaName()::equals).orElse(true))
                .collect(toImmutableList());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
        return getColumnHandles(session, ((RocketMQTableHandle) tableHandle).toSchemaTableName());
    }

    private Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, SchemaTableName schemaTableName) {
        RocketMQTopicDescription rocketMQTopicDescription = getRequiredTopicDescription(session, schemaTableName);
        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        AtomicInteger index = new AtomicInteger(0);
        rocketMQTopicDescription.getKey().ifPresent(key -> {
            List<RocketMQTopicFieldDescription> fields = key.getFields();
            if (fields != null) {
                for (RocketMQTopicFieldDescription topicFieldDescription : fields) {
                    columnHandles.put(topicFieldDescription.getName(), topicFieldDescription.getColumnHandle(true, index.getAndIncrement()));
                }
            }
        });

        rocketMQTopicDescription.getMessage().ifPresent(message -> {
            List<RocketMQTopicFieldDescription> fields = message.getFields();
            if (fields != null) {
                for (RocketMQTopicFieldDescription rocketMqTopicFieldDescription : fields) {
                    columnHandles.put(rocketMqTopicFieldDescription.getName(), rocketMqTopicFieldDescription.getColumnHandle(false, index.getAndIncrement()));
                }
            }
        });

        for (RocketMQInternalFieldManager.InternalField rocketmqInternalField : rocketMQInternalFieldManager.getInternalFields().values()) {
            columnHandles.put(rocketmqInternalField.getColumnName(), rocketmqInternalField.getColumnHandle(index.getAndIncrement(), hideInternalColumns));
        }

        return columnHandles.buildOrThrow();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix) {
        requireNonNull(prefix, "prefix is null");

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        List<SchemaTableName> tableNames;
        if (!prefix.getTable().isPresent()) {
            tableNames = listTables(session, prefix.getSchema());
        } else {
            tableNames = ImmutableList.of(prefix.toSchemaTableName());
        }

        for (SchemaTableName tableName : tableNames) {
            try {
                columns.put(tableName, getTableMetadata(session, tableName).getColumns());
            } catch (TableNotFoundException e) {
            }
        }
        return columns.buildOrThrow();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
        return ((RocketMQColumnHandle) columnHandle).getColumnMetadata();
    }

    private ConnectorTableMetadata getTableMetadata(ConnectorSession session, SchemaTableName schemaTableName) {
        RocketMQTopicDescription table = getRequiredTopicDescription(session, schemaTableName);
        ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();
        table.getKey().ifPresent(key -> {
            List<RocketMQTopicFieldDescription> fields = key.getFields();
            if (fields != null) {
                for (RocketMQTopicFieldDescription fieldDescription : fields) {
                    builder.add(fieldDescription.getColumnMetadata());
                }
            }
        });

        table.getMessage().ifPresent(message -> {
            List<RocketMQTopicFieldDescription> fields = message.getFields();
            if (fields != null) {
                for (RocketMQTopicFieldDescription fieldDescription : fields) {
                    builder.add(fieldDescription.getColumnMetadata());
                }
            }
        });

        for (RocketMQInternalFieldManager.InternalField fieldDescription : rocketMQInternalFieldManager.getInternalFields().values()) {
            builder.add(fieldDescription.getColumnMetadata(hideInternalColumns));
        }

        return new ConnectorTableMetadata(schemaTableName, builder.build());
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table) {
        return new ConnectorTableProperties();
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle table, Constraint constraint) {
        RocketMQTableHandle handle = (RocketMQTableHandle) table;
        TupleDomain<ColumnHandle> oldDomain = handle.getConstraint();
        TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(constraint.getSummary());
        if (oldDomain.equals(newDomain)) {
            return Optional.empty();
        }

        handle = new RocketMQTableHandle(
                handle.getSchemaName(),
                handle.getTableName(),
                handle.getTopicName(),
                handle.getKeyDataFormat(),
                handle.getMessageDataFormat(),
                handle.getKeySchemaLocation(),
                handle.getValueSchemaLocation(),
                handle.getKeySubject(),
                handle.getMessageSubject(),
                handle.getColumns(),
                newDomain);

        return Optional.of(new ConstraintApplicationResult<>(handle, constraint.getSummary(), false));
    }

    private RocketMQTopicDescription getRequiredTopicDescription(ConnectorSession session, SchemaTableName schemaTableName) {
        return getTopicDescription(session, schemaTableName).orElseThrow(() -> new TableNotFoundException(schemaTableName));
    }

    private Optional<RocketMQTopicDescription> getTopicDescription(ConnectorSession session, SchemaTableName schemaTableName) {
        return tableDescriptionSupplier.getTopicDescription(session, schemaTableName);
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode) {
        if (retryMode != NO_RETRIES) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support query retries");
        }
        RocketMQTableHandle table = (RocketMQTableHandle) tableHandle;
        List<RocketMQColumnHandle> actualColumns = table.getColumns().stream()
                .filter(columnHandle -> !columnHandle.isInternal() && !columnHandle.isHidden())
                .collect(toImmutableList());

        checkArgument(columns.equals(actualColumns), "Unexpected columns!\nexpected: %s\ngot: %s", actualColumns, columns);

        return new RocketMQTableHandle(
                table.getSchemaName(),
                table.getTableName(),
                table.getTopicName(),
                table.getKeyDataFormat(),
                table.getMessageDataFormat(),
                table.getKeySchemaLocation(),
                table.getValueSchemaLocation(),
                table.getKeySubject(),
                table.getMessageSubject(),
                actualColumns,
                TupleDomain.none());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics) {
        return Optional.empty();
    }

}
