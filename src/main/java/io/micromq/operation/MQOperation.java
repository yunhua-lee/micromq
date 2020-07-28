package io.micromq.operation;

import io.micromq.client.MQClient;
import io.micromq.common.MQException;
import io.micromq.config.PrivilegeConfig;
import io.micromq.config.QueueManager;
import io.micromq.model.Message;
import io.micromq.model.Receipt;
import io.micromq.operation.strategy.pull.NoPullPrivilege;
import io.micromq.operation.strategy.pull.PullWithAck;
import io.micromq.operation.strategy.pull.PullWithoutAck;
import io.micromq.operation.strategy.save.AsyncSave;
import io.micromq.operation.strategy.save.NoPublishPrivilege;
import io.micromq.operation.strategy.save.SyncSave;
import io.micromq.queue.MQQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MQOperation implements IPull, ISave {
    private static final Logger logger = LoggerFactory.getLogger(MQClient.class);

    private final MQClient client;
    private final MQQueue queue;

    private volatile IPull pullStrategy;
    private volatile ISave saveStrategy;

    private MQOperation(MQClient client, MQQueue queue){
        this.client = client;
        this.queue = queue;
    }

    public static MQOperation build(MQClient client, MQQueue queue){
        MQOperation operation = new MQOperation(client, queue);
        operation.loadPullStrategy();
        operation.loadSaveStrategy();

        return operation;
    }

    public String getClientName() {
        return client.getClientName();
    }

    public String getQueueName() {
        return queue.getName();
    }

    @Override
    public Message pull(long msgId) throws MQException {
        return pullStrategy.pull(msgId);
    }

    @Override
    public Integer ack(long msgId) throws MQException {
        return pullStrategy.ack(msgId);
    }

    @Override
    public Receipt getReceipt() throws MQException {
        return pullStrategy.getReceipt();
    }

    @Override
    public Boolean save(Message message) throws MQException {
        return saveStrategy.save(message);
    }

    public void loadPullStrategy(){
        if(!queue.isActive()){
            throw new RuntimeException("queue is not active: " + queue.getName());
        }

        if(!PrivilegeConfig.INSTANCE().hasPullPrivilege(getClientName(), getQueueName())){
            pullStrategy = new NoPullPrivilege();
            return;
        }

        int mode = queue.getPullMode();
        if( mode == QueueManager.PULL_WITHOUT_ACK ){
            pullStrategy =  PullWithoutAck.build(this);
            return;
        }else if(mode == QueueManager.PULL_WITH_ACK){
            pullStrategy = PullWithAck.build(this);
            return;
        }
        else {
            throw new RuntimeException("invalid queue ack mode: " + mode);
        }
    }

    public void loadSaveStrategy(){
        if(!queue.isActive()){
            throw new RuntimeException("queue is not active: " + queue.getName());
        }

        if(!PrivilegeConfig.INSTANCE().hasPublishPrivilege(getClientName(), getQueueName())){
            saveStrategy = new NoPublishPrivilege();
        }

        int mode = queue.getSaveMode();
        if(mode == QueueManager.SYNC_SAVE){
            saveStrategy = SyncSave.build();
        }else if(mode == QueueManager.ASYNC_SAVE){
            saveStrategy = AsyncSave.build(this);
        }
        else {
            throw new RuntimeException("invalid queue save mode: " + mode);
        }
    }
}
