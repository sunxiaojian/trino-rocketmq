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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.trino.plugin.rocketmq.schema.file.FileTableDescriptionSupplier;
import io.trino.spi.HostAddress;


public class RocketMqConfig {
    private static final int NAME_SRV_DEFAULT_PORT = 9876;
    private HostAddress nameSrvAddr = HostAddress.fromString("localhost:9876");
    private String rmqConsumeGroup = "RmqConsumeGroup";
    private String defaultSchema = "default";
    private boolean hideInternalColumns = true;
    private int messagesPerSplit = 100_000;
    private String tableDescriptionSupplier = FileTableDescriptionSupplier.NAME;

    /**
     * set acl config
     **/
    private boolean aclEnable;
    private String accessKey;
    private String secretKey;


    public String getRmqConsumeGroup() {
        return rmqConsumeGroup;
    }

    @Config("RocketMQ.rmq-consume-group")
    @ConfigDescription("")
    public RocketMqConfig setRmqConsumeGroup(String rmqConsumeGroup) {
        this.rmqConsumeGroup = rmqConsumeGroup;
        return this;
    }


    public HostAddress getNameSrvAddr() {
        return nameSrvAddr;
    }

    @Config("RocketMQ.name-srv-addr")
    @ConfigDescription("")
    public RocketMqConfig setNameSrvAddr(String nameSrvAddr) {
        this.nameSrvAddr = HostAddress.fromString(nameSrvAddr).withDefaultPort(NAME_SRV_DEFAULT_PORT);
        return this;
    }

    public String getDefaultSchema() {
        return defaultSchema;
    }

    @Config("RocketMQ.default-schema")
    @ConfigDescription("")
    public RocketMqConfig setDefaultSchema(String defaultSchema) {
        this.defaultSchema = defaultSchema;
        return this;
    }

    public boolean isHideInternalColumns() {
        return hideInternalColumns;
    }

    @Config("RocketMQ.hide-internal-columns")
    @ConfigDescription("Count of RocketMQ messages to be processed by single Trino RocketMQ connector split")
    public RocketMqConfig setHideInternalColumns(boolean hideInternalColumns) {
        this.hideInternalColumns = hideInternalColumns;
        return this;
    }

    public int getMessagesPerSplit() {
        return messagesPerSplit;
    }

    @Config("RocketMQ.messages-per-split")
    @ConfigDescription("Count of RocketMQ messages to be processed by single Trino rocketmq connector split")
    public RocketMqConfig setMessagesPerSplit(int messagesPerSplit) {
        this.messagesPerSplit = messagesPerSplit;
        return this;
    }

    public boolean isAclEnable() {
        return aclEnable;
    }

    @Config("RocketMQ.acl-enable")
    @ConfigDescription("")
    public RocketMqConfig setAclEnable(boolean aclEnable) {
        this.aclEnable = aclEnable;
        return this;
    }

    public String getTableDescriptionSupplier() {
        return tableDescriptionSupplier;
    }

    @Config("RocketMQ.table-description-supplier")
    @ConfigDescription("")
    public RocketMqConfig setTableDescriptionSupplier(String tableDescriptionSupplier) {
        this.tableDescriptionSupplier = tableDescriptionSupplier;
        return this;
    }

    public String getAccessKey() {
        return accessKey;
    }

    @Config("RocketMQ.access-key")
    @ConfigDescription("")
    public RocketMqConfig setAccessKey(String accessKey) {
        this.accessKey = accessKey;
        return this;
    }

    public String getSecretKey() {
        return secretKey;
    }
    @Config("RocketMQ.secret-key")
    public RocketMqConfig setSecretKey(String secretKey) {
        this.secretKey = secretKey;
        return this;
    }
}
