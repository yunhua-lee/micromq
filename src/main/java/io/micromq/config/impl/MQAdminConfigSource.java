package io.micromq.config.impl;

import io.micromq.config.ConfigSource;
import org.apache.commons.configuration.Configuration;

public class MQAdminConfigSource implements ConfigSource {
    private final String adminIp;
    private final int adminPort;

    public MQAdminConfigSource(String adminIp, int adminPort){
        this.adminIp = adminIp;
        this.adminPort = adminPort;
    }

    @Override
    public Configuration getConfig(String type) {
        //TODO: get configuration from MQAdmin server
        return null;
    }

    @Override
    public void addListener(String type, Listener listener) {
        //TODO
    }
}
