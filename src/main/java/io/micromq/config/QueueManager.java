package io.micromq.config;

import io.micromq.config.option.MQIntOption;
import io.micromq.config.option.MQOption;
import io.micromq.queue.MQQueue;
import org.apache.commons.lang3.Validate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class QueueManager implements ConfigSource.Listener{
    public static final int PULL_WITHOUT_ACK = 0;
    public static final int PULL_WITH_ACK = 1;
    public static final int SYNC_SAVE = 0;
    public static final int ASYNC_SAVE = 1;

    public static final String CONFIG_TYPE = "queue";
    private static volatile QueueManager instance;

    private final ConfigSource source;
    private final Map<String, MQQueue> queueMap = new ConcurrentHashMap<>();

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
            queueMap.put(name, queue);
        }

        return queue;
    }

    public List<MQQueue> getAllQueues(){
        return new ArrayList(queueMap.values());
    }

    private int getQueueAckMode(String queue) {
        MQOption<Integer> option = new MQIntOption().withName(queue + ".ackMode")
                .withCategory(CONFIG_TYPE).withDescription("queue ack mode")
                .withDefaultValue(1).withMinValue(PULL_WITHOUT_ACK).withMaxValue(PULL_WITH_ACK).parse(source);

        return option.value();
    }

    private int getQueueAckTimeout(String queue) {
        MQOption<Integer> option = new MQIntOption().withName(queue + ".ackTimeout")
                .withCategory(CONFIG_TYPE).withDescription("queue ack timeout threshold")
                .withDefaultValue(3).withMinValue(1).withMaxValue(30).parse(source);

        return option.value();
    }

    private int getQueueSaveMode(String queue){
        MQOption<Integer> option = new MQIntOption().withName(queue + "." + "savingMode")
                .withCategory(CONFIG_TYPE).withDescription("queue saving mode")
                .withDefaultValue(ASYNC_SAVE).withMinValue(0).withMaxValue(1).parse(source);

        return option.value();
    }

    private Boolean isActive(String queue) {
        MQOption<Integer> option = new MQIntOption().withName(queue + ".active")
                .withCategory(CONFIG_TYPE).withDescription("queue is active or not")
                .withDefaultValue(0).withMinValue(0).withMaxValue(1).parse(source);

        return option.value() == 1;
    }
}
