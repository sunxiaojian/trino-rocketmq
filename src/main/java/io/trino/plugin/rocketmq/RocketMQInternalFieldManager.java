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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;

import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TypeSignature.arrayType;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;

/**
 * RocketMQ internal field
 */
public class RocketMQInternalFieldManager {

    public static final String QUEUE_ID_FIELD = "_queue_id";

    public static final String BROKER_NAME = "_broker_name";

    public static final String QUEUE_OFFSET_FIELD = "_queue_offset";

    public static final String MSG_ID = "_msg_id";
    public static final String MESSAGE_FIELD = "_message";
    public static final String MESSAGE_LENGTH_FIELD = "_message_length";

    public static final String KEY_FIELD = "_key";

    public static final String KEY_LENGTH_FIELD = "_key_length";

    public static final String OFFSET_TIMESTAMP_FIELD = "_timestamp";

    public static final String MESSAGE_PROPERTIES = "_properties";

    private final Map<String, InternalField> internalFields;

    @Inject
    public RocketMQInternalFieldManager(TypeManager typeManager)
    {
        Type varcharMapType = typeManager.getType(mapType(VARCHAR.getTypeSignature(), arrayType(VARBINARY.getTypeSignature())));

        internalFields = ImmutableMap.<String, InternalField>builder()
                .put(QUEUE_ID_FIELD, new InternalField(
                        QUEUE_ID_FIELD,
                        "Queue Id",
                        BigintType.BIGINT))
                .put(QUEUE_OFFSET_FIELD, new InternalField(
                        QUEUE_OFFSET_FIELD,
                        "Offset for the message within the MessageQueue",
                        BigintType.BIGINT))
                .put(MESSAGE_FIELD, new InternalField(
                        MESSAGE_FIELD,
                        "Message text",
                        createUnboundedVarcharType()))
                .put(MESSAGE_LENGTH_FIELD, new InternalField(
                        MESSAGE_LENGTH_FIELD,
                        "Total number of message bytes",
                        BigintType.BIGINT))
                .put(KEY_FIELD, new InternalField(
                        KEY_FIELD,
                        "Key text",
                        createUnboundedVarcharType()))
                .put(KEY_LENGTH_FIELD, new InternalField(
                        KEY_LENGTH_FIELD,
                        "Total number of key bytes",
                        BigintType.BIGINT))
                .put(OFFSET_TIMESTAMP_FIELD, new InternalField(
                        OFFSET_TIMESTAMP_FIELD,
                        "Message timestamp",
                        TIMESTAMP_MILLIS))
                .put(MESSAGE_PROPERTIES, new InternalField(
                        MESSAGE_PROPERTIES,
                        "message properties",
                        varcharMapType
                ))
                .buildOrThrow();
    }

    public static class InternalField {
        private final String columnName;
        private final String comment;
        private final Type type;

        InternalField(String columnName, String comment, Type type)
        {
            this.columnName = requireNonNull(columnName, "columnName is null");
            this.comment = requireNonNull(comment, "comment is null");
            this.type = requireNonNull(type, "type is null");
        }

        public String getColumnName()
        {
            return columnName;
        }

        private Type getType()
        {
            return type;
        }

        RocketMQColumnHandle getColumnHandle(int index, boolean hidden) {
            return new RocketMQColumnHandle(
                    getColumnName(),
                    getType(),
                    null,
                    null,
                    null,
                    false,
                    hidden,
                    true);
        }

        ColumnMetadata getColumnMetadata(boolean hidden) {
            return ColumnMetadata.builder()
                    .setName(columnName)
                    .setType(type)
                    .setComment(Optional.ofNullable(comment))
                    .setHidden(hidden)
                    .build();
        }
    }

    public Map<String, InternalField> getInternalFields() {
        return internalFields;
    }
}
