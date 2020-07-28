package io.micromq.ha.standby;

import io.micromq.log.MQLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.*;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public abstract class ZkNode implements Watcher{
    private static final Logger logger = LoggerFactory.getLogger(ZkNode.class);

    protected volatile ZooKeeper zk;
    protected final HAConfig config;

    protected volatile boolean active = false;
    protected volatile boolean autoCheck = true;

    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

    public ZkNode(HAConfig config){
        this.config = config;
        executor.scheduleWithFixedDelay(new ZooKeeperAdmin(), 2, 60, TimeUnit.SECONDS);
    }

    public synchronized void connect() {
        if( zk != null && !zk.getState().equals(ZooKeeper.States.CLOSED)){
            return;
        }

        close();
        try{
            zk = new ZooKeeper(config.getZooKeeperAddress(), config.getZooKeeperTimeout(), this);
        }
        catch (Exception e){
            MQLog log = new MQLog("create ZooKeeper client exception")
                    .p("address", config.getZooKeeperAddress())
                    .p("timeout", config.getZooKeeperTimeout());
            logger.error(log.toString());
        }
    }

    public synchronized void close() {
        if( zk != null ){
            try {
                zk.close();
                zk = null;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public boolean isActive(){
        return active;
    }

    protected String makeNodePath(String node){
        return config.getZooKeeperPath() + "/" + config.getGroup() + "/" + node;
    }

    private class ZooKeeperAdmin implements Runnable{

        @Override
        public void run() {
            if(!autoCheck){
                MQLog log = new MQLog("ZooKeeperAdmin don't check connection state because autocheck is disabled");
                logger.warn(log.toString());

                return;
            }

            if( zk == null || zk.getState().equals(ZooKeeper.States.CLOSED)){
                connect();
            }
        }
    }
}
