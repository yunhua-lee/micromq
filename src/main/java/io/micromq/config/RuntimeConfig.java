package io.micromq.config;

import io.micromq.log.MQLog;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RuntimeConfig implements ConfigSource.Listener {
    private static final Logger logger = LoggerFactory.getLogger(RuntimeConfig.class);

    public static final String ASYNC_MESSAGE_SAVING_THREAD_NUMBER = "server.asyncMessageSavingThreadNumber";
    public static final String ASYNC_MESSAGE_SAVING_RATE = "server.asyncMessageSavingRate";
    public static final String ASYNC_MESSAGE_SAVING_PERIOD = "server.asyncMessageSavingPeriod";
    public static final String RECEIPT_SAVING_PERIOD = "server.receiptSavingPeriod";
    public static final String MESSAGE_EXPIRE_HOURS = "server.messageExpireHours";

    public static final int DEFAULT_PORT = 8080;

    public static final String CONFIG_TYPE = "runtime";
    private static volatile RuntimeConfig instance;

    private final ConfigSource source;

    private RuntimeConfig(ConfigSource source){
        this.source = source;
    }

    public static synchronized void init(ConfigSource source){
        instance = new RuntimeConfig(source);
    }

    @Override
    public void refresh(ConfigSource source) {
        RuntimeConfig.init(source);
    }

    public static RuntimeConfig INSTANCE(){
        return instance;
    }

    public int getAsyncMessageSavingThreadNumber() {
        int defaultValue;
        int processorNumber = Runtime.getRuntime().availableProcessors();

        if (processorNumber > 16) {
            defaultValue = 16;
        } else {
            defaultValue = processorNumber;
        }

        int number = source.getConfig(CONFIG_TYPE).getInt(ASYNC_MESSAGE_SAVING_THREAD_NUMBER, defaultValue);
        Validate.inclusiveBetween(1, 64, number, ASYNC_MESSAGE_SAVING_THREAD_NUMBER + " should be between 1 and 64");

        return number;
    }

    public int getAsyncMessageSavingRate() {
        int threadNumber = getAsyncMessageSavingThreadNumber();
        int defaultValue = 20000 / threadNumber;

        int rate = source.getConfig(CONFIG_TYPE).getInt(ASYNC_MESSAGE_SAVING_RATE, defaultValue);
        Validate.inclusiveBetween(100, 20000, rate, ASYNC_MESSAGE_SAVING_RATE + " should be between 100 and 20000");
        if (rate > 1000) {
            MQLog log = MQLog.build("Too large rate may cause MySQL 'max_allowed_packet' related error")
                    .p("config", ASYNC_MESSAGE_SAVING_RATE)
                    .p("value", rate);
            logger.warn(log.toString());
        }

        return rate;
    }

    public int getAsyncMessageSavingPeriod() {
        int period = source.getConfig(CONFIG_TYPE).getInt(ASYNC_MESSAGE_SAVING_PERIOD, 1000);
        Validate.inclusiveBetween(100, 2000, period, ASYNC_MESSAGE_SAVING_PERIOD + " should be between 100 and 2000(ms)");

        return period;
    }

    public int getReceiptSavingPeriod() {
        int period = source.getConfig(CONFIG_TYPE).getInt(RECEIPT_SAVING_PERIOD, 1000);
        Validate.inclusiveBetween(100, 2000, period, RECEIPT_SAVING_PERIOD + " should be between 100 and 2000(ms)");

        return period;
    }

    public int getMessageExpireHours(){
        int hours = source.getConfig(CONFIG_TYPE).getInt(MESSAGE_EXPIRE_HOURS, 48);
        Validate.inclusiveBetween(24, 7* 24, hours, MESSAGE_EXPIRE_HOURS + " should be between 24 and 168(hours)");

        return hours;
    }
}

