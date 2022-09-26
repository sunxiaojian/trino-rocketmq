package io.trino.plugin.rocketmq;

import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSession;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExtImpl;

/**
 * rocketmq consumer factory
 */
public interface RocketMQConsumerFactory {

    default DefaultLitePullConsumer create(ConnectorSession session)
    {
        return consumer(session);
    }

    DefaultLitePullConsumer consumer(ConnectorSession session);

    DefaultMQAdminExt admin(ConnectorSession session);

    HostAddress hostAddress();
}
