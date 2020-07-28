package io.micromq.config;

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
        int mode = source.getConfig(CONFIG_TYPE).getInteger(queue + ".ackMode", 1);

        Validate.inclusiveBetween(PULL_WITHOUT_ACK, PULL_WITH_ACK, mode);
        return mode;
    }

    private int getQueueAckTimeout(String queue) {
        int timeout = source.getConfig(CONFIG_TYPE).getInteger(queue + ".ackTimeout", 3);

        Validate.inclusiveBetween(1, 30, timeout, "ackTimeout should be between 1 and 30 seconds");
        return timeout;
    }

    private int getQueueSaveMode(String queue){
        String key = queue + "." + "savingMode";
        int mode = source.getConfig(CONFIG_TYPE).getInt(key, ASYNC_SAVE);
        Validate.inclusiveBetween(0, 1, mode, "savingMode should be 0(ASYNC_SAVE) or 1(SYNC_SAVE)");

        return mode;
    }

    private Boolean isActive(String queue) {
        int queueState = source.getConfig(CONFIG_TYPE).getInt(queue + ".active", 0);

        return queueState == 1;
    }
}
