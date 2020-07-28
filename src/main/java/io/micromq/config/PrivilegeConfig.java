package io.micromq.config;

import org.apache.commons.lang3.Validate;

public final class PrivilegeConfig implements ConfigSource.Listener {
    private static final String PUBLISH_PRIVILEGE = "publish";
    private static final String PULL_PRIVILEGE = "pull";

    public static final String CONFIG_TYPE = "privilege";
    private static volatile PrivilegeConfig instance;

    private final ConfigSource source;

    private PrivilegeConfig(ConfigSource source){
        this.source = source;
    }

    public static synchronized void init(ConfigSource source){
        instance = new PrivilegeConfig(source);
    }

    @Override
    public void refresh(ConfigSource source) {
        PrivilegeConfig.init(source);
    }

    public static PrivilegeConfig INSTANCE(){
        return instance;
    }

    public Boolean hasPublishPrivilege(String client, String queue) {
        return hasPrivilege(client, queue, PUBLISH_PRIVILEGE);
    }

    public Boolean hasPullPrivilege(String client, String queue) {
        return hasPrivilege(client, queue, PULL_PRIVILEGE);
    }

    private Boolean hasPrivilege(String client, String queue, String privilege) {
        Validate.notBlank(client, "client is blank");
        Validate.notBlank(queue, "queue is blank");

        String key = client + "." + queue + "." + privilege;

        int value = source.getConfig(CONFIG_TYPE).getInt(key, 0);

        return value == 1;
    }
}
