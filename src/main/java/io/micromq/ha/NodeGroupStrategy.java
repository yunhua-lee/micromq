package io.micromq.ha;

import io.micromq.server.MQNode;

public interface NodeGroupStrategy {
    void start();
    void stop();
    boolean supportPush();
    boolean supportPull();
    void setNode(MQNode node);
    MQNode getNode();
    String getName();
}
