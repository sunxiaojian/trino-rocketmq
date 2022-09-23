package io.trino.plugin.rocketmq.split;

import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;

public class RocketMQSplit implements ConnectorSplit {
    @Override
    public boolean isRemotelyAccessible() {
        return false;
    }

    @Override
    public List<HostAddress> getAddresses() {
        return null;
    }

    @Override
    public Object getInfo() {
        return null;
    }

}
