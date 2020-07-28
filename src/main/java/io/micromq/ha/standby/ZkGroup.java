package io.micromq.ha.standby;

import io.micromq.config.ConfigSource;
import io.micromq.ha.NodeGroupStrategy;
import io.micromq.server.MQNode;

public class ZkGroup implements NodeGroupStrategy {

    private volatile MQNode node;
    private final ZkNode role;
    private final HAConfig config;

    public static ZkGroup build(ConfigSource source){
        HAConfig config = new HAConfig(source.getConfig("ha"));
        String roleConfig = config.getRole();

        ZkNode role;
        if(HAConfig.ZK_MASTER.equalsIgnoreCase(roleConfig)){
            role = new ZkMaster(config);
        }
        else if(HAConfig.ZK_SLAVE.equalsIgnoreCase(roleConfig)){
            role = new ZkSlave(config);
        }
        else{
            throw new RuntimeException("invalid role config: " + roleConfig);
        }

        return new ZkGroup(role, config);
    }

    private ZkGroup(ZkNode role, HAConfig config){
        this.role = role;
        this.config = config;
    }

    @Override
    public void start() {
        role.connect();
    }

    @Override
    public void stop() {
        role.close();
    }

    @Override
    public boolean supportPush() {
        if(role instanceof ZkMaster && role.isActive()){
            return true;
        }

        return false;
    }

    @Override
    public boolean supportPull() {
        return role.isActive();
    }

    @Override
    public void setNode(MQNode node) {
        this.node = node;
    }

    @Override
    public MQNode getNode() {
        return node;
    }

    @Override
    public String getName() {
        return "zookeeper standby";
    }

    private static String makeNodePath(String zkPath, String group, String role){
        return zkPath + "/" + group + "/" + role;
    }
}
