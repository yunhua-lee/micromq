package io.micromq.config;

import io.micromq.config.option.MQIntOption;
import io.micromq.config.option.MQOption;

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
        String key = client + "." + queue + "." + privilege;

        MQOption<Integer> option = new MQIntOption().withName(key).withDescription("client has queue privilege or not")
                .withDefaultValue(0).withMinValue(0).withMaxValue(1)
                .parse(source.getConfig(CONFIG_TYPE));

        return option.value() == 1;
    }
}
