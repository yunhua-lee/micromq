package io.micromq.server;

import io.micromq.common.MQCode;
import io.micromq.common.MQException;
import io.micromq.config.ConfigSource;
import io.micromq.config.ServerConfig;
import io.micromq.dao.IMessageDao;
import io.micromq.dao.IReceiptDao;
import io.micromq.dao.impl.mysql.MySQLMessageDao;
import io.micromq.dao.impl.mysql.MySQLReceiptDao;
import io.micromq.ha.NodeGroupStrategy;
import io.micromq.ha.single.MQSingle;
import io.micromq.ha.standby.ZkGroup;
import io.micromq.server.task.MsgClearingTask;
import io.micromq.server.task.ReceiptSavingTask;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

public class MQNode implements ConfigSource.Listener {
    private static final Logger logger = LoggerFactory.getLogger(MQNode.class);

    public static final String CONFIG_TYPE = "mqnode";

    private static volatile MQNode instance;
    private NodeGroupStrategy strategy;

    private IMessageDao messageDao;
    private IReceiptDao receiptDao;

    private volatile boolean active;

    private MQNode(){
    }

    public void setStrategy(NodeGroupStrategy strategy) {
        this.strategy = strategy;
    }

    public static synchronized MQNode build(ConfigSource source) throws MQException {
        if(instance != null){
            logger.warn("MQNode already started!");
            return instance;
        }

        instance = new MQNode();

        //node group strategy
        NodeGroupStrategy strategy;

        String strategyType = source.getConfig("ha").getString("ha");
        if(StringUtils.isBlank(strategyType)){
            strategy = new MQSingle();
        }
        else if(strategyType.equalsIgnoreCase("zk")){
            strategy = ZkGroup.build(source);
        }
        else {
            throw new MQException(MQCode.INVALID_CONFIG, "invalid config for ha: " + strategyType);
        }
        strategy.setNode(instance);
        instance.setStrategy(strategy);

        logger.info("MQNode group strategy: " + strategy.getName());

        //storage
        String storageType = source.getConfig("storage").getString("type");
        logger.info("MQNode storage type: " + storageType);

        if("mysql".equalsIgnoreCase(storageType)){
            DriverManagerDataSource dataSource =  new DriverManagerDataSource();
            dataSource.setDriverClassName("com.mysql.jdbc.Driver");
            dataSource.setUrl(source.getConfig("storage").getString("datasource.url"));
            dataSource.setUsername(source.getConfig("storage").getString("datasource.username"));
            dataSource.setPassword(source.getConfig("storage").getString("datasource.password"));

            instance.messageDao = new MySQLMessageDao(dataSource);
            instance.receiptDao = new MySQLReceiptDao(dataSource);
        }

        //star tasks
        logger.info("MQNode start task: " + ReceiptSavingTask.class);
        ReceiptSavingTask.start(source);

        logger.info("MQNode start task: " + MsgClearingTask.class);
        MsgClearingTask.start(source);

        return instance;
    }

    @Override
    public void refresh(ConfigSource source) {
        ReceiptSavingTask.stop();
        ReceiptSavingTask.start(source);

        MsgClearingTask.stop();
        MsgClearingTask.start(source);
    }

    public static MQNode INSTANCE(){
        return instance;
    }

    public void start() {
        if(active){
            return;
        }

        strategy.start();
        logger.info("MQNode started group strategy");

        active = true;
        logger.info("MQNode now is started");
    }

    public void stop(){
        if(!active){
            return;
        }

        strategy.stop();
        logger.info("MQNode stopped group strategy");

        active = false;
        logger.info("MQNode now is stopped");
    }

    public boolean isActive(){
        return active;
    }

    public boolean supportPublish(){
        return active && strategy.supportPush();
    }

    public boolean supportPull(){
        return active && strategy.supportPull();
    }

    public String getName(){
        return ServerConfig.INSTANCE().getServerName();
    }

    public IMessageDao getMessageDao() {
        return messageDao;
    }

    public IReceiptDao getReceiptDao() {
        return receiptDao;
    }
}
