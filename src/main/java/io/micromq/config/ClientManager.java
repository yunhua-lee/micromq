package io.micromq.config;

import io.micromq.client.MQClient;
import io.micromq.common.MQException;
import io.micromq.config.option.MQIntOption;
import io.micromq.config.option.MQOption;
import io.micromq.config.option.MQStringOption;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class ClientManager implements ConfigSource.Listener {
    public static final String CONFIG_TYPE = "client";
    private static volatile ClientManager instance;

    private final ConfigSource source;

    private ClientManager(ConfigSource source){
        this.source = source;
    }

    public static synchronized void init(ConfigSource source){
        Validate.notNull(source);

        instance = new ClientManager(source);
    }

    private static final Map<String, MQClient> clientMap = new ConcurrentHashMap<>();

    @Override
    public void refresh(ConfigSource source) {
        clientMap.clear();
        ClientManager.init(source);
    }

    public static ClientManager INSTANCE(){
        return instance;
    }

    public MQClient buildClient(String name){
        String clientName = formatName(name);

        MQClient client = new MQClient(clientName);
        client.setActive(isActive(clientName));
        client.setSignKey(getSignKey(clientName));

        return client;
    }

    public MQClient getClient(String name) throws MQException {
        String clientName = formatName(name);

        MQClient client = clientMap.get(clientName);
        if(client == null){
            client = buildClient(clientName);
            clientMap.putIfAbsent(clientName, client);
            client = clientMap.get(clientName);
        }

        return client;
    }

    public List<MQClient> getAllClients(){
        return new ArrayList(clientMap.values());
    }

    private Boolean isActive(String name) {
        String clientName = formatName(name);
        String key = clientName + ".active";

        MQOption<Integer> option = new MQIntOption().withName(key).withCategory(CONFIG_TYPE)
                .withDescription("client is active or not").withDefaultValue(0)
                .withMinValue(0).withMaxValue(1).parse(source);

        return option.value() == 1;
    }

    private String getSignKey(String name) {
        String clientName = formatName(name);
        String signKey = clientName + "." + "signKey";

        MQOption<String> option = new MQStringOption().withName(signKey).withCategory(CONFIG_TYPE)
                .withDescription("sign key for client").withNoDefaultValue().parse(source);

        return option.value();
    }

    private String formatName(String name){
        Validate.notEmpty(name);
        return StringUtils.trim(name);
    }
}
