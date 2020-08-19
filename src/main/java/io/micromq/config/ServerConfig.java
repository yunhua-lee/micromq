package io.micromq.config;

import io.micromq.config.option.*;
import io.micromq.util.SysUtils;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;

public final class ServerConfig {
    private static final Logger logger = LoggerFactory.getLogger(ServerConfig.class);

    public static final String SERVER_NAME = "server.name";
    public static final String SERVER_PORT = "server.port";
    public static final String SERVER_ROLE = "server.role";

    public static final String GROUP_SERVER = "group";
    public static final String SINGLE_SERVER = "single";

    public static final int DEFAULT_PORT = 8080;

    private static volatile ServerConfig instance;

    private Configuration configuration;

    private ServerConfig(Configuration configuration){
        this.configuration = configuration;
    }

    public static synchronized void build(Configuration configuration){
        if(instance != null){
            throw new RuntimeException("server config can not be dynamically refreshed");
        }
        instance = new ServerConfig(configuration);
    }

    public static ServerConfig INSTANCE(){
        return instance;
    }

    public String getServerName() {
        String defaultName = SysUtils.getHostName() + ":" + getPort();

        return new MQStringOption().withName(SERVER_NAME)
                .withDescription("server name").withDefaultValue(defaultName)
                .parse(configuration)
                .value();
    }

    public int getPort() {
        return new MQIntOption().withName(SERVER_PORT)
                .withDescription("queue ack timeout threshold")
                .withDefaultValue(DEFAULT_PORT).withMinValue(1024).withMaxValue(65535)
                .parse(configuration)
                .value();
    }

    public String getServerType() {
        return new MQStringEnumOption().withName(SERVER_ROLE)
                .withDescription("server role")
                .withDefaultValue(SINGLE_SERVER)
                .withValidValues(new HashSet<String>() {
                    {
                        add(SINGLE_SERVER);
                        add(GROUP_SERVER);
                    }
                })
                .parse(configuration)
                .value();
    }

    public String getAdminIP(){
        //TODO
        return "";
    }

    public int getAdminPort(){
        //TODO
        return 8080;
    }
}

