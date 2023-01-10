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
package io.trino.plugin.rocketmq.split;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.SizeOf;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

/**
 * RocketMQ split
 */
public class RocketMqSplit implements ConnectorSplit {

    private static final long INSTANCE_SIZE = ClassLayout.parseClass(RocketMqSplit.class).instanceSize();
    private final String topicName;
    private final String keyDataFormat;
    private final String messageDataFormat;
    private final Optional<String> keyDataSchemaContents;
    private final Optional<String> messageDataSchemaContents;
    private final int queueId;
    private final String brokerName;
    private final Range messagesRange;
    private final HostAddress nameSrvAddress;

    @JsonCreator
    public RocketMqSplit(@JsonProperty("topicName") String topicName,
                         @JsonProperty("keyDataFormat") String keyDataFormat,
                         @JsonProperty("messageDataFormat") String messageDataFormat,
                         @JsonProperty("keyDataSchemaContents") Optional<String> keyDataSchemaContents,
                         @JsonProperty("messageDataSchemaContents") Optional<String> messageDataSchemaContents,
                         @JsonProperty("queueId") int queueId,
                         @JsonProperty("brokerName") String brokerName,
                         @JsonProperty("messagesRange") Range messagesRange,
                         @JsonProperty("nameSrvAddress") HostAddress nameSrvAddress) {

        this.topicName = requireNonNull(topicName, "topicName is null");
        this.keyDataFormat = requireNonNull(keyDataFormat, "keyDataFormat is null");
        this.messageDataFormat = requireNonNull(messageDataFormat, "messageDataFormat is null");
        this.keyDataSchemaContents = keyDataSchemaContents;
        this.messageDataSchemaContents = messageDataSchemaContents;
        this.queueId = queueId;
        this.brokerName = brokerName;
        this.messagesRange = requireNonNull(messagesRange, "messagesRange is null");
        this.nameSrvAddress = requireNonNull(nameSrvAddress, "namesrv is null");
    }


    @Override
    public boolean isRemotelyAccessible() {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses() {
        return ImmutableList.of(nameSrvAddress);
    }

    @Override
    public Object getInfo() {
        return this;
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
    public Optional<String> getKeyDataSchemaContents() {
        return keyDataSchemaContents;
    }

    @JsonProperty
    public Optional<String> getMessageDataSchemaContents() {
        return messageDataSchemaContents;
    }

    @JsonProperty
    public int getQueueId() {
        return queueId;
    }

    @JsonProperty
    public Range getMessagesRange() {
        return messagesRange;
    }

    @JsonProperty
    public HostAddress getNameSrvAddress() {
        return nameSrvAddress;
    }

    @JsonProperty
    public String getBrokerName() {
        return brokerName;
    }


    @Override
    public long getRetainedSizeInBytes() {
        return INSTANCE_SIZE
                + estimatedSizeOf(topicName)
                + estimatedSizeOf(keyDataFormat)
                + estimatedSizeOf(messageDataFormat)
                + sizeOf(keyDataSchemaContents, SizeOf::estimatedSizeOf)
                + sizeOf(messageDataSchemaContents, SizeOf::estimatedSizeOf)
                + messagesRange.getRetainedSizeInBytes()
                + nameSrvAddress.getRetainedSizeInBytes();
    }


    @Override
    public String toString() {
        return toStringHelper(this)
                .add("topicName", topicName)
                .add("keyDataFormat", keyDataFormat)
                .add("messageDataFormat", messageDataFormat)
                .add("keyDataSchemaContents", keyDataSchemaContents)
                .add("messageDataSchemaContents", messageDataSchemaContents)
                .add("queueId", queueId)
                .add("messagesRange", messagesRange)
                .add("nameSrvAddress", nameSrvAddress)
                .toString();
    }
}
