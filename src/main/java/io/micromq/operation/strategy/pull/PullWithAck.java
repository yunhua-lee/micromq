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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class PullWithAck implements IPull {
    private static final Logger logger = LoggerFactory.getLogger(PullWithAck.class);

    private static final int ACK_BUFFER_SIZE = 1024;
    private static final int MAX_TIMEOUT_COUNT = 3;
    private static final int ACK_TIMEOUT_THRESHOLD = 3000; //TODO:config

    private final MQOperation queue;
    private volatile IMessageDao messageDao;
    private volatile IReceiptDao receiptDao;

    private final AtomicBoolean moveCheckpointLock = new AtomicBoolean(false);
    private volatile long lastMessageId = 0;
    private volatile long checkpoint;
    private volatile long nextMsgId;

    private volatile Long lastTimeoutCheckTime;
    private final AtomicBoolean timeoutCheck = new AtomicBoolean(false);

    private static final Long MESSAGE_ACK = 0L;
    private static final Long MESSAGE_NO_SEND = -1L;
    private static final Long MESSAGE_LOST = -2L;

    private final Map<Long, Long> ackBuffer = new ConcurrentHashMap<>(ACK_BUFFER_SIZE);
    private final Map<Long, Integer> timeoutCountBuffer = new ConcurrentHashMap<>();

    private PullWithAck(MQOperation queue) {
        this.queue = queue;
        this.messageDao = MQNode.INSTANCE().getMessageDao();
        this.receiptDao = MQNode.INSTANCE().getReceiptDao();
    }

    public static PullWithAck build(MQOperation queue){
        return new PullWithAck(queue);
    }

    public Message pull(long msgId) throws MQException {
        Validate.isTrue(msgId >= 0);

        Message message;

        //get specified message
        if (msgId > 0) {
            return messageDao.getMessage(queue.getQueueName(), msgId);
        }

        checkAckBuffer();

        //resend timeout message
        Long timeoutMsgId = resendTimeoutMessage();
        if (timeoutMsgId != null) {
            message = messageDao.getMessage(queue.getQueueName(), timeoutMsgId);

            if (message == null) {
                processLostMessage(timeoutMsgId);
            } else {
                MQLog log = new MQLog("resend timeout message")
                        .p("msgId", timeoutMsgId);
                logger.warn(log.toString());
                ackBuffer.put(timeoutMsgId, System.currentTimeMillis());

                return message;
            }
        }

        //pull newest message
        msgId = getNextMsgId();
        if( msgId == 0 ){
            return null;
        }

        message = messageDao.getMessage(queue.getQueueName(), msgId);

        if (message != null) {
            ackBuffer.put(msgId, System.currentTimeMillis());
        } else {
            ackBuffer.put(msgId, MESSAGE_LOST);
        }

        return message;
    }

    @Override
    public Integer ack(long msgId) throws MQException{
        Validate.isTrue(msgId > 0);

        //repeated ack message
        if (msgId <= checkpoint) {
            MQLog log = new MQLog("repeated ack message behind checkpoint")
                    .p("checkpoint", checkpoint)
                    .p("msgId", msgId);
            logger.error(log.toString());

            return IPullErrCode.ACK_REPEATED;
        }

        //disordered ack message
        if (msgId > lastMessageId) {
            MQLog log = new MQLog("disordered ack message")
                    .p("checkpoint", checkpoint)
                    .p("msgId", msgId);
            logger.error(log.toString());

            return IPullErrCode.ACK_DISORDERED;
        }

        //message is already acked
        Long pullTime = ackBuffer.get(msgId);
        if (pullTime == null) {
            MQLog log = new MQLog("repeated ack message")
                    .p("checkpoint", checkpoint)
                    .p("msgId", msgId);
            logger.error(log.toString());

            return IPullErrCode.ACK_REPEATED;
        }

        //timeout ack message
        Long currentTime = System.currentTimeMillis();
        if (currentTime - pullTime > ACK_TIMEOUT_THRESHOLD) {
            MQLog log = new MQLog("timeout ack message")
                    .p("pullTime", pullTime)
                    .p("currentTime", currentTime)
                    .p("msgId", msgId);
            logger.error(log.toString());

            return IPullErrCode.ACK_TIMEOUT;
        }

        ackBuffer.put(msgId, MESSAGE_ACK);
        timeoutCountBuffer.remove(msgId);

        return IPullErrCode.ACK_SUCCESS;
    }

    @Override
    public Receipt getReceipt() {
        return new Receipt(queue.getClientName(), queue.getQueueName(), checkpoint);
    }

    private Long resendTimeoutMessage() {
        Long currentTime = System.currentTimeMillis();

        //no need to check timeout acked
        if (lastTimeoutCheckTime== null || currentTime - lastTimeoutCheckTime < ACK_TIMEOUT_THRESHOLD) {
            return null;
        }

        //only one thread check timeout acked message
        if (!timeoutCheck.compareAndSet(false, true)) {
            return null;
        }

        try {
            lastTimeoutCheckTime = currentTime;

            long msgId = checkpoint + 1;

            Long pullTime = ackBuffer.get(msgId);
            if (pullTime == null || pullTime <= MESSAGE_ACK) {
                // move checkpoint forward
                moveCheckpoint();
                return null;
            } else {
                long timeout = currentTime - pullTime;
                if (timeout >= ACK_TIMEOUT_THRESHOLD) {
                    MQLog log = new MQLog("message timeout")
                            .p("msgId", msgId)
                            .p("pullTime", pullTime)
                            .p("currentTime", currentTime);
                    logger.warn(log.toString());

                    Integer timeoutCount = timeoutCountBuffer.get(msgId);
                    if (timeoutCount == null) {
                        timeoutCountBuffer.put(msgId, 1);
                    } else if (timeoutCount >= MAX_TIMEOUT_COUNT) {
                        //some messages will cause consumer exception each time,
                        //so we don't resend them again
                        log.setMessage("too many timeout, no send again")
                                .p("msgId", msgId)
                                .p("pullTime", pullTime)
                                .p("currentTime", currentTime)
                                .p("count", timeoutCount);
                        logger.error(log.toString());

                        ackBuffer.put(msgId, MESSAGE_NO_SEND);
                        timeoutCountBuffer.remove(msgId);

                        return null;
                    } else {
                        timeoutCountBuffer.put(msgId, timeoutCount + 1);
                    }

                    //resend timeout message
                    return msgId;
                } else {
                    return null;
                }
            }
        } finally {
            Validate.isTrue(timeoutCheck.compareAndSet(true, false));
        }
    }

    private void checkAckBuffer() throws MQException {
        //ack buffer is full, not allowed to pull message
        Integer bufferSize = ackBuffer.size();
        if (bufferSize >= ACK_BUFFER_SIZE) {
            MQLog log = new MQLog("ack buffer is full")
                    .p("lastMessageId", lastMessageId)
                    .p("checkpoint", checkpoint)
                    .p("buffer size", bufferSize);
            logger.warn(log.toString());

            throw new MQException(MQCode.EXCEPTION, "ack buffer is full, size: " + ACK_BUFFER_SIZE);
        }
    }

    private void moveCheckpoint() {
        //only one thread can move checkpoint
        if(!moveCheckpointLock.compareAndSet(false, true)){
            return;
        }

        try {
            while (true) {
                Long ack = ackBuffer.get(checkpoint + 1);
                if (ack == null) {
                    break;
                }

                if (ack <= MESSAGE_ACK) {
                    //only one thread run this code, it's safe to use ++ operator.
                    ackBuffer.remove(++checkpoint);
                } else {
                    break;
                }
            }
        }
        finally {
            Validate.isTrue(moveCheckpointLock.compareAndSet(true, false));
        }
    }

    private void processLostMessage(long msgId) {
        MQLog log = new MQLog("message lost").p("msgId", msgId);
        logger.error(log.toString());

        ackBuffer.put(msgId, MESSAGE_LOST);
        timeoutCountBuffer.remove(msgId);
    }

    private synchronized void updateLastMessageId() {
        Message lastMessage = messageDao.getLastMessage(queue.getQueueName());
        if (lastMessage == null) {
            lastMessageId = 0;
        } else {
            if( lastMessage.getMsgId() > lastMessageId ) {
                lastMessageId = lastMessage.getMsgId();
            }
        }
    }

    private synchronized long getNextMsgId(){
        if(lastMessageId == 0 || nextMsgId > lastMessageId){
            updateLastMessageId();

            if(lastMessageId < checkpoint){
                checkpoint = lastMessageId;
            }

            if(lastMessageId == 0 || nextMsgId > lastMessageId){
                nextMsgId = lastMessageId + 1;
                return 0;
            }
        }

        return nextMsgId++;
    }
}
