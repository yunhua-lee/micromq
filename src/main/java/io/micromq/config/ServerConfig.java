package io.micromq.config;

import io.micromq.log.MQLog;
import io.micromq.util.SysUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        String name = configuration.getString(SERVER_NAME);
        if (StringUtils.isBlank(name)) {
            // If not configured, use "host:port"
            name = SysUtils.getHostName() + ":" + getPort();
        } else {
            name = StringUtils.trim(name);
        }

        return name;
    }

    public int getPort() {
        int port = configuration.getInt(SERVER_PORT, DEFAULT_PORT);
        Validate.inclusiveBetween(1024, 65535, port, "port should be between 1024 and 65535");

        return port;
    }

    public String getServerType() {
        String role = configuration.getString(SERVER_ROLE, SINGLE_SERVER);
        if (GROUP_SERVER.equals(role)
                || SINGLE_SERVER.equals(role)) {
            return role;
        }

        MQLog log = new MQLog("invalid config")
                .p("name", SERVER_ROLE)
                .p("value", role)
                .p("valid values", GROUP_SERVER + "," + SINGLE_SERVER);

        throw new IllegalArgumentException(log.toString());
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

