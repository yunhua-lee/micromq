package io.micromq.ha.single;

import io.micromq.ha.NodeGroupStrategy;
import io.micromq.server.MQNode;

public class MQSingle implements NodeGroupStrategy {
    private volatile MQNode node;
    private volatile boolean active;

    public MQSingle(){
        this.active = true;
    }

    @Override
    public void start() {
        active = true;
    }

    @Override
    public void stop() {
        active = false;
    }

    @Override
    public boolean supportPush() {
        return active;
    }

    @Override
    public boolean supportPull() {
        return active;
    }

    @Override
    public void setNode(MQNode node) {
        this.node = node;
        return;
    }

    @Override
    public MQNode getNode() {
        return node;
    }

    @Override
    public String getName() {
        return "single";
    }
}
