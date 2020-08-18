package io.micromq.config;

import io.micromq.config.option.MQIntOption;
import io.micromq.config.option.MQOption;
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

        MQOption<Integer> option = new MQIntOption().withName(ASYNC_MESSAGE_SAVING_THREAD_NUMBER)
                .withCategory(CONFIG_TYPE).withDescription("async message saving thread number")
                .withDefaultValue(defaultValue).withMinValue(1).withMaxValue(64).parse(source);

        return option.value();
    }

    public int getAsyncMessageSavingRate() {
        int threadNumber = getAsyncMessageSavingThreadNumber();
        int defaultValue = 20000 / threadNumber;

        MQOption<Integer> option = new MQIntOption().withName(ASYNC_MESSAGE_SAVING_RATE)
                .withCategory(CONFIG_TYPE).withDescription("async message saving rate")
                .withDefaultValue(defaultValue).withMinValue(100).withMaxValue(20000).parse(source);

        if (option.value() > 1000) {
            MQLog log = MQLog.build("Too large rate may cause MySQL 'max_allowed_packet' related error")
                    .p("config", ASYNC_MESSAGE_SAVING_RATE)
                    .p("value", option.value());
            logger.warn(log.toString());
        }

        return option.value();
    }

    public int getAsyncMessageSavingPeriod() {
        MQOption<Integer> option = new MQIntOption().withName(ASYNC_MESSAGE_SAVING_PERIOD)
                .withCategory(CONFIG_TYPE).withDescription("async message saving period")
                .withDefaultValue(1000).withMinValue(100).withMaxValue(2000).parse(source);

        return option.value();
    }

    public int getReceiptSavingPeriod() {
        MQOption<Integer> option = new MQIntOption().withName(RECEIPT_SAVING_PERIOD)
                .withCategory(CONFIG_TYPE).withDescription("receipt saving period")
                .withDefaultValue(1000).withMinValue(100).withMaxValue(2000).parse(source);

        return option.value();
    }

    public int getMessageExpireHours(){
        MQOption<Integer> option = new MQIntOption().withName(MESSAGE_EXPIRE_HOURS)
                .withCategory(CONFIG_TYPE).withDescription("message expired hours")
                .withDefaultValue(48).withMinValue(24).withMaxValue(7 * 24).parse(source);

        return option.value();
    }
}

