package io.micromq.model;

import java.sql.Timestamp;

public class Receipt {
    private String client;
    private String queue;
    private long msgId;
    private Timestamp time;

    public Receipt(){
    }

    public Receipt(String client, String queue, long msgId){
        this.client = client;
        this.queue = queue;
        this.msgId = msgId;

        this.time = new Timestamp(System.currentTimeMillis());
    }

    public Receipt(String client, String queue, long msgId, Timestamp time){
        this.client = client;
        this.queue = queue;
        this.msgId = msgId;
        this.time = time;
    }

    public String getClient() {
        return client;
    }

    public void setClient(String client) {
        this.client = client;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public long getMsgId() {
        return msgId;
    }

    public void setMsgId(long msgId) {
        this.msgId = msgId;
    }

    public Timestamp getTime() {
        return time;
    }

    public void setTime(Timestamp time) {
        this.time = time;
    }
}
