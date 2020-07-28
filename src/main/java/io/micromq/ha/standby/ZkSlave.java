package io.micromq.ha.standby;

import io.micromq.log.MQLog;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkSlave extends ZkNode{
    private static final Logger logger = LoggerFactory.getLogger(ZkSlave.class);

    public ZkSlave(HAConfig config) {
        super(config);
    }

    @Override
    public void process(WatchedEvent watchedEvent){
        MQLog log = MQLog.build();
        String masterNodePath = makeNodePath(HAConfig.ZK_MASTER);

        //TODO should the watcher be registered any time ?
        try {
            if (watchedEvent.getState() == Watcher.Event.KeeperState.SyncConnected) {
                switch (watchedEvent.getType()){
                    case None:
                        String nodePath = makeNodePath(HAConfig.ZK_SLAVE);

                        zk.delete(nodePath, -1);
                        zk.create(nodePath, HAConfig.ZK_SLAVE.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

                        if( null == zk.exists(masterNodePath, true) ){
                            active = true;

                            //QueueService service = AppUtils.getService(QueueService.class);
                            //service.clearPullStrategy();

                            log.setMessage("slave is isActive");
                            logger.info(log.toString());
                        }else{
                            active = false;

                            log.setMessage("slave is not active because master is active");
                            logger.info(log.toString());
                        }
                        break;
                    case NodeCreated:
                        String path = watchedEvent.getPath();
                        String data = new String(zk.getData(path, false, null));

                        log.setMessage("NodeCreated").p("path", path).p("data", data);
                        logger.info(log.toString());

                        if( path.equals(masterNodePath) ){
                            active = false;

                            log.setMessage("master is isActive, slave changes to inactive").p("path", path).p("data",data);
                            logger.info(log.toString());
                        }
                        break;
                    case NodeDataChanged:
                        path = watchedEvent.getPath();
                        data = new String(zk.getData(path, false, null));
                        log.setMessage("NodeDataChanged").p("path", path).p("data", data);
                        logger.info(log.toString());

                        break;
                    case NodeDeleted:
                        path = watchedEvent.getPath();
                        log.setMessage("NodeDeleted").p("path", path);
                        logger.info(log.toString());

                        if( path.equals(masterNodePath) ){
                            active = true;

                            log.setMessage("master is inactive, slave changes to active")
                                    .p("path", path);
                            logger.info(log.toString());
                        }

                        break;
                    case NodeChildrenChanged:
                        path = watchedEvent.getPath();
                        log = new MQLog("NodeChildrenChanged")
                                .p("path", path);
                        logger.info(log.toString());

                        break;
                    default:
                        throw new RuntimeException("unsupported ZooKeeper event type: " + watchedEvent.getType());
                }

                return;
            }

            if (watchedEvent.getState() == Watcher.Event.KeeperState.Disconnected) {
                active = false;

                log.setMessage("Disconnected from ZooKeeper cluster").p("address", config.getZooKeeperAddress());
                logger.warn(log.toString());

                return;
            }

            if (watchedEvent.getState() == Watcher.Event.KeeperState.Expired) {
                active = false;

                log.setMessage("ZooKeeper session expired").p("address", config.getZooKeeperAddress());
                logger.warn(log.toString());

                return;
            }
        }
        catch (Exception e){
            active = false;

            try {
                zk.close();
                zk = null;
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
            log.setMessage("close ZooKeeper connection because of event process exception");
            log.p("exception", e.getMessage());

            logger.error(log.toString());
            logger.error(e.getMessage(), e);
        }
    }
}
