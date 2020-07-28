package io.micromq.operation.strategy.pull;

import io.micromq.common.MQCode;
import io.micromq.common.MQException;
import io.micromq.dao.IMessageDao;
import io.micromq.dao.IReceiptDao;
import io.micromq.model.Message;
import io.micromq.model.Receipt;
import io.micromq.log.MQLog;
import io.micromq.operation.MQOperation;
import io.micromq.server.MQNode;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.micromq.operation.IPull;

import java.util.concurrent.atomic.AtomicLong;

public class PullWithoutAck implements IPull {
    private static final Logger logger = LoggerFactory.getLogger(PullWithoutAck.class);

    private volatile IMessageDao messageDao;
    private volatile IReceiptDao receiptDao;

    private final MQOperation queue;
    private final AtomicLong receiptId = new AtomicLong(0);
    private volatile Long lastMessageId = 0L;

    private PullWithoutAck(MQOperation queue) {
        this.queue = queue;
        this.messageDao = MQNode.INSTANCE().getMessageDao();
        this.receiptDao = MQNode.INSTANCE().getReceiptDao();
    }

    public static PullWithoutAck build(MQOperation queue){
        PullWithoutAck strategy = new PullWithoutAck(queue);
        strategy.init();

        return strategy;
    }

    private void init(){
        Long id;
        Receipt receipt = receiptDao.getReceipt(queue.getClientName(), queue.getQueueName());
        if(receipt != null){
            id = receipt.getMsgId();
        }
        else {
            Message message = messageDao.getFirstMessage(queue.getQueueName());
            id = message == null ? 0 : message.getMsgId();
        }

        receiptId.set(id);
        updateLastMessageId();
    }

    @Override
    public Message pull(long msgId) throws MQException {
        Validate.isTrue(msgId >= 0);

        //pull specified message
        if (msgId > 0) {
            return messageDao.getMessage(queue.getQueueName(), msgId);
        }

        //msg == 0, pull last message
        long currentMessageId = receiptId.get();
        if(currentMessageId >= lastMessageId){
            updateLastMessageId();
        }

        //no new message
        currentMessageId = receiptId.get();
        if(currentMessageId >= lastMessageId){
            return null;
        }

        Long messageId = receiptId.addAndGet(1);
        return messageDao.getMessage(queue.getQueueName(), messageId);
    }

    @Override
    public Integer ack(long msgId) throws MQException{
        MQLog log = new MQLog("ack request for PullWithoutAck queue")
                .p("queue", queue)
                .p("msgId", msgId);
        logger.warn(log.toString());

        throw new MQException(MQCode.UNSUPPORTED, "ack request for PullWithoutAck queue");
    }

    @Override
    public Receipt getReceipt() {
        return new Receipt(queue.getClientName(), queue.getQueueName(), receiptId.get());
    }

    private synchronized void updateLastMessageId(){
        if(lastMessageId > receiptId.get()){
            return;
        }

        Message lastMessage = messageDao.getLastMessage(queue.getQueueName());
        if(lastMessage == null){
            lastMessageId = 0L;
        }
        else {
            lastMessageId = lastMessage.getMsgId();
        }

        //for manually delete some messages from storage
        if(receiptId.get() > lastMessageId){
            receiptId.set(lastMessageId);
        }
    }
}
