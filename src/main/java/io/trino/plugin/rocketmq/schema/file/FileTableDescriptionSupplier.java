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
package io.trino.plugin.rocketmq.schema.file;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.trino.decoder.dummy.DummyRowDecoder;
import io.trino.plugin.rocketmq.RocketMqConfig;
import io.trino.plugin.rocketmq.schema.BasedTableDescriptionSupplier;
import io.trino.plugin.rocketmq.schema.RocketMqTopicDescription;
import io.trino.plugin.rocketmq.schema.RocketMqTopicFieldGroup;
import io.trino.plugin.rocketmq.schema.TableDescriptionSupplier;
import io.trino.spi.connector.SchemaTableName;

import javax.inject.Inject;
import javax.inject.Provider;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.nio.file.Files.readAllBytes;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

public class FileTableDescriptionSupplier implements Provider<TableDescriptionSupplier> {
    private static final Logger log = Logger.get(FileTableDescriptionSupplier.class);
    public static final String NAME = "file";
    private final JsonCodec<RocketMqTopicDescription> topicDescriptionCodec;
    private final File tableDescriptionDir;
    private final String defaultSchema;
    private final Set<String> tableNames;

    @Inject
    FileTableDescriptionSupplier(FileTableDescriptionSupplierConfig config, RocketMqConfig rocketMQConfig, JsonCodec<RocketMqTopicDescription> topicDescriptionCodec) {
        this.topicDescriptionCodec = requireNonNull(topicDescriptionCodec, "Topic description codec is null");
        this.tableDescriptionDir = config.getTableDescriptionDir();
        this.defaultSchema = rocketMQConfig.getDefaultSchema();
        this.tableNames = ImmutableSet.copyOf(config.getTableNames());
    }

    @Override
    public TableDescriptionSupplier get() {
        Map<SchemaTableName, RocketMqTopicDescription> tables = populateTables();
        return new BasedTableDescriptionSupplier(tables);
    }

    /**
     * Populate tables
     * @return
     */
    private Map<SchemaTableName, RocketMqTopicDescription> populateTables() {
        ImmutableMap.Builder<SchemaTableName, RocketMqTopicDescription> builder = ImmutableMap.builder();
        log.debug("Loading rocketmq table definitions from %s", tableDescriptionDir.getAbsolutePath());

        try {
            for (File file : listFiles(tableDescriptionDir)) {
                // Parse json file
                if (file.isFile() && file.getName().endsWith(".json")) {
                    RocketMqTopicDescription table = topicDescriptionCodec.fromJson(readAllBytes(file.toPath()));
                    String schemaName = table.getSchemaName().orElse(defaultSchema);
                    log.debug("RocketMq table %s.%s: %s", schemaName, table.getTableName(), table);
                    builder.put(new SchemaTableName(schemaName, table.getTableName()), table);
                }
            }

            Map<SchemaTableName, RocketMqTopicDescription> tableDefinitions = builder.buildOrThrow();
            log.debug("Loaded Table definitions: %s", tableDefinitions.keySet());
            builder = ImmutableMap.builder();
            for (String definedTable : tableNames) {
                SchemaTableName tableName = parseTableName(definedTable);
                if (tableDefinitions.containsKey(tableName)) {
                    RocketMqTopicDescription rocketmqTable = tableDefinitions.get(tableName);
                    log.debug("Found Table definition for %s: %s", tableName, rocketmqTable);
                    builder.put(tableName, rocketmqTable);
                } else {
                    // A dummy table definition only supports the internal columns.
                    log.debug("Created dummy Table definition for %s", tableName);
                    builder.put(
                            tableName,
                            new RocketMqTopicDescription(
                                    tableName.getTableName(),
                                    Optional.ofNullable(tableName.getSchemaName()),
                                    definedTable,
                                    Optional.of(new RocketMqTopicFieldGroup(DummyRowDecoder.NAME, Optional.empty(), Optional.empty(), ImmutableList.of())),
                                    Optional.of(new RocketMqTopicFieldGroup(DummyRowDecoder.NAME, Optional.empty(), Optional.empty(), ImmutableList.of())))
                    );
                }
            }

            return builder.buildOrThrow();
        } catch (IOException e) {
            log.warn(e, "Failed to get table description files for RocketMq");
            throw new UncheckedIOException(e);
        }
    }

    private static List<File> listFiles(File dir)
    {
        if ((dir != null) && dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                log.debug("Considering files: %s", asList(files));
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }

    /**
     * Parse table name
     * @param schemaTableName
     * @return
     */
    private SchemaTableName parseTableName(String schemaTableName) {
        checkArgument(!isNullOrEmpty(schemaTableName), "schemaTableName is null or is empty");
        List<String> parts = Splitter.on('.').splitToList(schemaTableName);
        checkArgument(parts.size() == 2, "Invalid schemaTableName: %s", schemaTableName);

        if(parts.size() == 2 ){
            return new SchemaTableName(parts.get(0), parts.get(1));
        } else {
            return new SchemaTableName(defaultSchema, schemaTableName);
        }
    }
}
