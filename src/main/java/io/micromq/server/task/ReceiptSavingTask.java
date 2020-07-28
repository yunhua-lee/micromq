package io.micromq.server.task;

import io.micromq.client.MQClient;
import io.micromq.config.ClientManager;
import io.micromq.config.ConfigSource;
import io.micromq.config.RuntimeConfig;
import io.micromq.dao.IReceiptDao;
import io.micromq.model.Receipt;
import io.micromq.log.MQLog;
import io.micromq.operation.MQOperation;
import io.micromq.server.MQNode;
import io.micromq.util.SysUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ReceiptSavingTask implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(ReceiptSavingTask.class);

    private static volatile ReceiptSavingTask instance;
    private static volatile ScheduledExecutorService pool = Executors.newSingleThreadScheduledExecutor(
            SysUtils.createThreadFactory(ReceiptSavingTask.class.getCanonicalName()));

    public static synchronized void start(ConfigSource source){
        if(instance != null){
            MQLog log = new MQLog("ReceiptSavingTask is already running");
            logger.warn(log.toString());

            return;
        }
        int period = RuntimeConfig.INSTANCE().getReceiptSavingPeriod();

        instance = new ReceiptSavingTask();

        pool.scheduleWithFixedDelay(instance, 2000, period, TimeUnit.MILLISECONDS);

        MQLog log = new MQLog("ReceiptSavingTask starts to run").p("period(ms)", period);
        logger.info(log.toString());
    }

    public static synchronized void stop(){
        if(instance == null){
            return;
        }

        pool.shutdown();
        pool = null;
        logger.info("ReceiptSavingTask thread pool shutdown……");

        instance = null;
        logger.info("ReceiptSavingTask stopped.");
    }

    @Override
    public void run() {
        if(logger.isDebugEnabled()){
            MQLog log = new MQLog(ReceiptSavingTask.class.getCanonicalName() + " starts to run......")
                    .p("threadId", Thread.currentThread().getId());
            logger.debug(log.toString());
        }

        try {
            IReceiptDao dao = MQNode.INSTANCE().getReceiptDao();
            List<MQClient> clientList = ClientManager.INSTANCE().getAllClients();
            for(MQClient client : clientList){
                List<MQOperation> operationList = client.getAllOperations();
                for(MQOperation operation : operationList){
                    Receipt receipt = operation.getReceipt();
                    if(receipt != null) {
                        dao.updateReceipt(receipt);
                    }
                }
            }

        }catch (Throwable t){
            logger.error(t.getMessage(), t);
        }
    }
}
