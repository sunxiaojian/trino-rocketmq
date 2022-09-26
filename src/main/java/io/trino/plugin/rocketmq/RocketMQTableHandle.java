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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class RocketMQTableHandle implements ConnectorTableHandle, ConnectorInsertTableHandle {

    /**
     * The schema name used by Trino
     */
    private final String schemaName;
    /**
     * The table name used by Trino.
     */
    private final String tableName;

    /**
     * The topic name that is read from RocketMQ.
     */
    private final String topicName;
    /**
     * key data format
     */
    private final String keyDataFormat;

    /**
     * message data format
     */
    private final String messageDataFormat;
    /**
     * key data schema location
     */
    private final Optional<String> keyDataSchemaLocation;
    /**
     * Message data schema location
     */
    private final Optional<String> messageDataSchemaLocation;
    private final Optional<String> keySubject;
    private final Optional<String> messageSubject;
    private final List<RocketMQColumnHandle> columns;
    private final TupleDomain<ColumnHandle> constraint;

    @JsonCreator
    public RocketMQTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("topicName") String topicName,
            @JsonProperty("keyDataFormat") String keyDataFormat,
            @JsonProperty("messageDataFormat") String messageDataFormat,
            @JsonProperty("keyDataSchemaLocation") Optional<String> keyDataSchemaLocation,
            @JsonProperty("messageDataSchemaLocation") Optional<String> messageDataSchemaLocation,
            @JsonProperty("keySubject") Optional<String> keySubject,
            @JsonProperty("messageSubject") Optional<String> messageSubject,
            @JsonProperty("columns") List<RocketMQColumnHandle> columns,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint) {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.topicName = requireNonNull(topicName, "topicName is null");
        this.keyDataFormat = requireNonNull(keyDataFormat, "keyDataFormat is null");
        this.messageDataFormat = requireNonNull(messageDataFormat, "messageDataFormat is null");
        this.keyDataSchemaLocation = requireNonNull(keyDataSchemaLocation, "keyDataSchemaLocation is null");
        this.messageDataSchemaLocation = requireNonNull(messageDataSchemaLocation, "messageDataSchemaLocation is null");
        this.keySubject = requireNonNull(keySubject, "keySubject is null");
        this.messageSubject = requireNonNull(messageSubject, "messageSubject is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.constraint = requireNonNull(constraint, "constraint is null");
    }

    @JsonProperty
    public String getSchemaName() {
        return schemaName;
    }

    @JsonProperty
    public String getTableName() {
        return tableName;
    }

    @JsonProperty
    public String getTopicName() {
        return topicName;
    }

    @JsonProperty
    public String getKeyDataFormat() {
        return keyDataFormat;
    }

    @JsonProperty
    public String getMessageDataFormat() {
        return messageDataFormat;
    }

    @JsonProperty
    public Optional<String> getMessageDataSchemaLocation() {
        return messageDataSchemaLocation;
    }

    @JsonProperty
    public Optional<String> getKeyDataSchemaLocation() {
        return keyDataSchemaLocation;
    }

    @JsonProperty
    public Optional<String> getKeySubject() {
        return keySubject;
    }

    @JsonProperty
    public Optional<String> getMessageSubject() {
        return messageSubject;
    }

    @JsonProperty
    public List<RocketMQColumnHandle> getColumns() {
        return columns;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint() {
        return constraint;
    }

    public SchemaTableName toSchemaTableName() {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                schemaName,
                tableName,
                topicName,
                keyDataFormat,
                messageDataFormat,
                keyDataSchemaLocation,
                messageDataSchemaLocation,
                keySubject,
                messageSubject,
                columns,
                constraint);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        RocketMQTableHandle other = (RocketMQTableHandle) obj;
        return Objects.equals(this.schemaName, other.schemaName)
                && Objects.equals(this.tableName, other.tableName)
                && Objects.equals(this.topicName, other.topicName)
                && Objects.equals(this.keyDataFormat, other.keyDataFormat)
                && Objects.equals(this.messageDataFormat, other.messageDataFormat)
                && Objects.equals(this.keyDataSchemaLocation, other.keyDataSchemaLocation)
                && Objects.equals(this.messageDataSchemaLocation, other.messageDataSchemaLocation)
                && Objects.equals(this.keySubject, other.keySubject)
                && Objects.equals(this.messageSubject, other.messageSubject)
                && Objects.equals(this.columns, other.columns)
                && Objects.equals(this.constraint, other.constraint);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("schemaName", schemaName)
                .add("tableName", tableName)
                .add("topicName", topicName)
                .add("keyDataFormat", keyDataFormat)
                .add("messageDataFormat", messageDataFormat)
                .add("keyDataSchemaLocation", keyDataSchemaLocation)
                .add("messageDataSchemaLocation", messageDataSchemaLocation)
                .add("keySubject", keySubject)
                .add("messageSubject", messageSubject)
                .add("columns", columns)
                .add("constraint", constraint)
                .toString();
    }
}