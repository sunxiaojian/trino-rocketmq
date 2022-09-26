package io.trino.plugin.rocketmq;

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
 * rocketmq consumer factory
 */
public class DefaultRocketMQConsumerFactory implements RocketMQConsumerFactory{

    private RocketMQConfig config;
    @Inject
    public DefaultRocketMQConsumerFactory(RocketMQConfig rocketMQConfig){
        this.config = rocketMQConfig;
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
        consumer.setAutoCommit(false);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
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

    private static RPCHook getAclRPCHook(String accessKey, String secretKey) {
        return new AclClientRPCHook(new SessionCredentials(accessKey, secretKey));
    }
}
