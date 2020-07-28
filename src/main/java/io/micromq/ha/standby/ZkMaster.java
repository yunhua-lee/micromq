package io.micromq.ha.standby;

import io.micromq.log.MQLog;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkMaster extends ZkNode{
    private static final Logger logger = LoggerFactory.getLogger(ZkMaster.class);

    public ZkMaster(HAConfig config) {
        super(config);
    }

    @Override
    public void process(WatchedEvent watchedEvent){
        MQLog log = MQLog.build();

        //TODO should the watcher be registered any time ?
        try {
            if (watchedEvent.getState() == Watcher.Event.KeeperState.SyncConnected) {
                switch (watchedEvent.getType()){
                    case None:
                        String masterNodePath = makeNodePath(HAConfig.ZK_SLAVE);

                        zk.delete(masterNodePath, -1); //after restart, the original node may not be expired
                        zk.create(masterNodePath, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

                        active = true;

                        //QueueService service = SpringUtils.getBean(QueueService.class);
                        //QueueService service = AppUtils.getService(QueueService.class);
                        //service.clearPullStrategy();

                        break;
                    case NodeCreated:
                        String path = watchedEvent.getPath();
                        String data = new String(zk.getData(path, false, null));
                        log.setMessage("NodeCreated").p("path", path).p("data", data);
                        logger.info(log.toString());

                        break;
                    case NodeDataChanged:
                        path = watchedEvent.getPath();
                        data = new String(zk.getData(path, false, null));
                        log.setMessage("NodeDataChanged").p("path", path).p("masterNodeData", data);
                        logger.info(log.toString());

                        break;
                    case NodeDeleted:
                        path = watchedEvent.getPath();
                        log.setMessage("NodeDeleted").p("path", path);
                        logger.info(log.toString());

                        break;
                    case NodeChildrenChanged:
                        path = watchedEvent.getPath();
                        log.setMessage("NodeChildrenChanged").p("path", path);
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
