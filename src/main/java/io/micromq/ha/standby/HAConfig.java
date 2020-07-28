package io.micromq.ha.standby;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.Validate;

public class HAConfig {
    public static final String ZK_ADDRESS = "zk.address";
    public static final String ZK_TIMEOUT = "zk.timeout";
    public static final String ZK_PATH = "zk.path";
    public static final String ZK_GROUP = "zk.group";
    public static final String ZK_ROLE = "zk.role";
    public static final String ZK_MASTER = "master";
    public static final String ZK_SLAVE = "slave";

    public static final int DEFAULT_ZK_TIMEOUT = 60; //seconds

    private final Configuration configuration;

    public HAConfig(Configuration configuration ) {
        this.configuration = configuration;
    }

    public String getZooKeeperAddress() {
        String value = configuration.getString(ZK_ADDRESS, "");
        Validate.notBlank(value, ZK_ADDRESS + " is blank");

        return value;
    }

    public int getZooKeeperTimeout() {
        return configuration.getInt(ZK_TIMEOUT, DEFAULT_ZK_TIMEOUT);
    }

    public String getZooKeeperPath() {
        String value = configuration.getString(ZK_PATH, "");
        Validate.notBlank(value, ZK_PATH + " is blank");

        return value;
    }

    public String getGroup() {
        String value = configuration.getString(ZK_GROUP, "");
        Validate.notBlank(value, ZK_GROUP + " is blank");

        return value;
    }

    public String getRole() {
        String value = configuration.getString(ZK_ROLE, "");
        Validate.notBlank(value, ZK_GROUP + " is blank");

        return value;
    }
}
