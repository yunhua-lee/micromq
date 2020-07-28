package io.micromq.server.task;

import io.micromq.config.ConfigSource;
import io.micromq.config.QueueManager;
import io.micromq.config.RuntimeConfig;
import io.micromq.dao.IMessageDao;
import io.micromq.log.MQLog;
import io.micromq.queue.MQQueue;
import io.micromq.server.MQNode;
import io.micromq.util.SysUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MsgClearingTask implements Runnable{
    private static final Logger logger = LoggerFactory.getLogger(MsgClearingTask.class);

    private static volatile MsgClearingTask instance;

    private final int expiredThreshold;
    private static volatile ScheduledExecutorService pool = Executors.newScheduledThreadPool(1,
            SysUtils.createThreadFactory(MsgClearingTask.class.getCanonicalName()));

    public static synchronized void start(ConfigSource source){
        if(instance != null){
            MQLog log = new MQLog("MsgClearingTask is already running");
            logger.warn(log.toString());

            return;
        }
        int threshold = RuntimeConfig.INSTANCE().getMessageExpireHours();
        instance = new MsgClearingTask(threshold);
        pool.scheduleWithFixedDelay(instance, 60, 60, TimeUnit.SECONDS);

        MQLog log = new MQLog("MsgClearingTask starts to run").p("threshold", threshold);
        logger.info(log.toString());
    }

    public static synchronized void stop(){
        if(instance == null){
            return;
        }

        pool.shutdown();
        pool = null;
        logger.info("MsgClearingTask thread pool shutdown……");

        instance = null;
        logger.info("MsgClearingTask stopped.");
    }

    private MsgClearingTask(int threshold){
        this.expiredThreshold = threshold;
    }

    @Override
    public void run() {
        try {
            MQLog log = new MQLog("MsgClearingTask begins")
                    .p("thread id", Thread.currentThread().getId())
                    .p("expired threshold", expiredThreshold);
            logger.info(log.toString());

            Integer total = 0;
            Date date = new Date(System.currentTimeMillis() - expiredThreshold * 3600 * 1000);
            Timestamp until = new Timestamp(date.getTime());

            IMessageDao dao = MQNode.INSTANCE().getMessageDao();
            List<MQQueue> queueList = QueueManager.INSTANCE().getAllQueues();
            for(MQQueue queue : queueList) {
                if(!queue.isActive()){
                    continue;
                }

                while (true) {
                    Integer result = dao.deleteMessages(queue.getName(), until, 2000);
                    if (result == 0) {
                        break;
                    }

                    total += result;
                    Thread.sleep(1000);
                }
            }

            log.setMessage("MsgClearingTask is finished")
                    .p("thread id", Thread.currentThread().getId())
                    .p("expired threshold", expiredThreshold)
                    .p("total", total);
            logger.info(log.toString());

        } catch (Throwable t) {
            logger.error(t.getMessage(), t);
        }
    }
}
