package io.micromq.config;

import io.micromq.config.option.MQIntEnumOption;
import io.micromq.config.option.MQIntOption;
import io.micromq.config.option.MQOption;
import io.micromq.queue.MQQueue;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class QueueManager implements ConfigSource.Listener{
    public static final int PULL_WITHOUT_ACK = 0;
    public static final int PULL_WITH_ACK = 1;
    public static final int SYNC_SAVE = 0;
    public static final int ASYNC_SAVE = 1;

    public static final String CONFIG_TYPE = "queue";
    private static volatile QueueManager instance;

    private final ConfigSource source;
    private final ConcurrentHashMap<String, MQQueue> queueMap = new ConcurrentHashMap<>();

    private QueueManager(ConfigSource source){
        this.source = source;
    }

    public static synchronized void init(ConfigSource source){
        instance = new QueueManager(source);
    }

    @Override
    public void refresh(ConfigSource source) {
        queueMap.clear();
        QueueManager.init(source);
    }

    public static QueueManager INSTANCE(){
        return instance;
    }

    public MQQueue getQueue(String name){
        MQQueue queue = queueMap.get(name);
        if(queue == null){
            queue = new MQQueue(name);
            queue.setActive(isActive(name));
            queue.setPullMode(getQueueAckMode(name));
            queue.setSaveMode(getQueueSaveMode(name));

            MQQueue old = queueMap.putIfAbsent(name, queue);
            if( old != null ){
                queue = old;
            }
        }

        return queue;
    }

    public List<MQQueue> getAllQueues(){
        return new ArrayList(queueMap.values());
    }

    private int getQueueAckMode(String queue) {
        return new MQIntEnumOption().withName(queue + ".ackMode")
                .withDescription("queue ack mode")
                .withDefaultValue(PULL_WITHOUT_ACK)
                .withValidValues(new HashSet<Integer>() {
                    {
                        add(PULL_WITHOUT_ACK);
                        add(PULL_WITH_ACK);
                    }
                })
                .parse(source.getConfig(CONFIG_TYPE))
                .value();
    }

    private int getQueueAckTimeout(String queue) {
        return new MQIntOption().withName(queue + ".ackTimeout")
                .withDescription("queue ack timeout threshold")
                .withDefaultValue(3).withMinValue(1).withMaxValue(30)
                .parse(source.getConfig(CONFIG_TYPE))
                .value();
    }

    private int getQueueSaveMode(String queue){
        return new MQIntOption().withName(queue + "." + "savingMode")
                .withDescription("queue saving mode")
                .withDefaultValue(ASYNC_SAVE).withMinValue(0).withMaxValue(1)
                .parse(source.getConfig(CONFIG_TYPE))
                .value();
    }

    private Boolean isActive(String queue) {
        MQOption<Integer> option = new MQIntOption().withName(queue + ".active")
                .withDescription("queue is active or not")
                .withDefaultValue(0).withMinValue(0).withMaxValue(1)
                .parse(source.getConfig(CONFIG_TYPE));

        return option.value() == 1;
    }
}
