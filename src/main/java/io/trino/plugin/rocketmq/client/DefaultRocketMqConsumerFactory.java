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

package io.trino.plugin.rocketmq.client;

import io.trino.plugin.rocketmq.RocketMqConfig;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSession;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

import javax.inject.Inject;

/**
 * Default rocketMq consumer factory
 */
public class DefaultRocketMqConsumerFactory implements RocketMqConsumerFactory {

    private final RocketMqConfig config;

    @Inject
    public DefaultRocketMqConsumerFactory(RocketMqConfig rocketMqConfig) {
        this.config = rocketMqConfig;
    }

    private static RPCHook getAclRPCHook(String accessKey, String secretKey) {
        return new AclClientRPCHook(new SessionCredentials(accessKey, secretKey));
    }

    @Override
    public DefaultLitePullConsumer consumer(ConnectorSession session) {
        DefaultLitePullConsumer consumer;
        if (StringUtils.isBlank(config.getAccessKey()) && StringUtils.isBlank(config.getSecretKey())) {
            consumer = new DefaultLitePullConsumer();
        } else {
            consumer = new DefaultLitePullConsumer(getAclRPCHook(config.getAccessKey(), config.getSecretKey()));
        }
        consumer.setNamesrvAddr(config.getNameSrvAddr().toString());
        String uniqueName = Thread.currentThread().getName() + "-" + System.currentTimeMillis() % 1000;
        consumer.setInstanceName(uniqueName);
        consumer.setUnitName(uniqueName);
        // disabled auto commit
        consumer.setAutoCommit(true);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setConsumerGroup(config.getRmqConsumeGroup());
        return consumer;
    }

    @Override
    public DefaultMQAdminExt admin(ConnectorSession session) {
        DefaultMQAdminExt admin;
        try {
            if (config.isAclEnable()) {
                admin = new DefaultMQAdminExt(new AclClientRPCHook(new SessionCredentials(config.getAccessKey(), config.getSecretKey())));
            } else {
                admin = new DefaultMQAdminExt();
            }
            admin.setNamesrvAddr(config.getNameSrvAddr().toString());
            admin.setAdminExtGroup(config.getRmqConsumeGroup());
            String uniqueName = Thread.currentThread().getName() + "-" + System.currentTimeMillis() % 1000;
            admin.setInstanceName(uniqueName);
            admin.start();
        } catch (MQClientException e) {
            throw new RuntimeException(e);
        }
        return admin;
    }

    @Override
    public HostAddress hostAddress() {
        return this.config.getNameSrvAddr();
    }
}
