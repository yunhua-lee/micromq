package io.micromq.operation.strategy.save;

import io.micromq.common.MQException;
import io.micromq.config.RuntimeConfig;
import io.micromq.dao.IMessageDao;
import io.micromq.model.Message;
import io.micromq.log.MQLog;
import io.micromq.operation.MQOperation;
import io.micromq.operation.ISave;
import io.micromq.server.MQNode;
import io.micromq.util.SysUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class AsyncSave implements ISave {

    private final MQOperation queue;
    private static volatile MsgAsyncSavingTask task;

    private AsyncSave(MQOperation queue){
        this.queue = queue;
    }

    public static synchronized AsyncSave build(MQOperation queue){
        if(task == null){
            synchronized (AsyncSave.class) {
                if(task == null){
                    int threadNumber = RuntimeConfig.INSTANCE().getAsyncMessageSavingThreadNumber();
                    int rate = RuntimeConfig.INSTANCE().getAsyncMessageSavingRate();
                    int period = RuntimeConfig.INSTANCE().getReceiptSavingPeriod();

                    task = new MsgAsyncSavingTask();
                    task.start(threadNumber, rate, period);
                }
            }
        }

        return new AsyncSave(queue);
    }

    @Override
    public Boolean save(Message message) throws MQException{
        return task.submit(message);
    }

    static class MsgAsyncSavingTask {
        private static final Logger logger = LoggerFactory.getLogger(MsgAsyncSavingTask.class);

        private static final BlockingQueue<Message> buffer = new ArrayBlockingQueue<Message>(10 * 1024);
        private static ScheduledExecutorService pool;

        void start(int threadNumber, int rate, int period) {
            if( pool != null ){
                MQLog log = new MQLog("MsgAsyncSavingTask is already running");
                logger.warn(log.toString());

                return;
            }

            int total = threadNumber * rate;
            if (total > 50000) {
                MQLog log = new MQLog("dangerous config for AsyncSave because threadNumber * rate is larger than 50000")
                        .p("threadNumber", threadNumber)
                        .p("rate", rate);
                logger.warn(log.toString());
            }

            pool = Executors.newScheduledThreadPool(threadNumber,
                    SysUtils.createThreadFactory(MsgAsyncSavingTask.class.getCanonicalName()));

            for (int i = 0; i < threadNumber; i++) {
                pool.scheduleWithFixedDelay(new TaskRunnable(rate),
                        2000, period, TimeUnit.MILLISECONDS);
            }
        }

        boolean submit(Message message) {
            Validate.notNull(message);

            int waitTime = 3000; //ms
            try {
                boolean result = buffer.offer(message, waitTime, TimeUnit.MILLISECONDS);
                if (!result) {
                    MQLog log = new MQLog("AsyncSave buffer is full")
                            .p("buffer size", buffer.size())
                            .p("wait time", waitTime);
                    logger.error(log.toString());
                }

                return result;
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                return false;
            }
        }

        class TaskRunnable implements Runnable {

            private final IMessageDao messageDao;

            private final int rate;

            TaskRunnable(int rate){
                this.rate = rate;
                messageDao = MQNode.INSTANCE().getMessageDao();
            }

            @Override
            public void run() {
                List<Message> messages = new ArrayList<Message>();
                int number = buffer.drainTo(messages, rate);

                int saveNumber = messageDao.saveMessages(messages);

                MQLog log = new MQLog("obtain messages to save")
                        .p("number", number)
                        .p("saveNumber", saveNumber)
                        .p("thread", Thread.currentThread().getId());
                logger.debug(log.toString());
            }
        }
    }
}
