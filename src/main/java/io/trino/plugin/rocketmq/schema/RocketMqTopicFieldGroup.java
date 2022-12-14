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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * Groups the field descriptions for message or key.
 */
public class RocketMqTopicFieldGroup {
    private final String dataFormat;
    private final Optional<String> dataSchema;
    private final Optional<String> subject;
    private final List<RocketMqTopicFieldDescription> fields;

    @JsonCreator
    public RocketMqTopicFieldGroup(
            @JsonProperty("dataFormat") String dataFormat,
            @JsonProperty("dataSchema") Optional<String> dataSchema,
            @JsonProperty("subject") Optional<String> subject,
            @JsonProperty("fields") List<RocketMqTopicFieldDescription> fields)
    {
        this.dataFormat = requireNonNull(dataFormat, "dataFormat is null");
        this.dataSchema = requireNonNull(dataSchema, "dataSchema is null");
        this.subject = requireNonNull(subject, "subject is null");
        this.fields = ImmutableList.copyOf(requireNonNull(fields, "fields is null"));
    }

    @JsonProperty
    public String getDataFormat()
    {
        return dataFormat;
    }

    @JsonProperty
    public List<RocketMqTopicFieldDescription> getFields()
    {
        return fields;
    }

    @JsonProperty
    public Optional<String> getDataSchema()
    {
        return dataSchema;
    }

    @JsonProperty
    public Optional<String> getSubject()
    {
        return subject;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(dataFormat, dataSchema, subject, fields);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        RocketMqTopicFieldGroup other = (RocketMqTopicFieldGroup) obj;
        return Objects.equals(this.dataFormat, other.dataFormat) &&
                Objects.equals(this.dataSchema, other.dataSchema) &&
                Objects.equals(this.subject, other.subject) &&
                Objects.equals(this.fields, other.fields);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("dataFormat", dataFormat)
                .add("dataSchema", dataSchema)
                .add("subject", subject)
                .add("fields", fields)
                .toString();
    }
}
