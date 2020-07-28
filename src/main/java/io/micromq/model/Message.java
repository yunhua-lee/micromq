package io.micromq.model;

import java.sql.Timestamp;

public final class Message {
    private long msgId;
    private String queue;
    private String content;
    private String client;
    private Timestamp createTime;

    public long getMsgId() {
        return msgId;
    }

    public Message setMsgId(long msgId) {
        this.msgId = msgId;
        return this;
    }

    public String getQueue() {
        return queue;
    }

    public Message setQueue(String queue) {
        this.queue = queue;
        return this;
    }

    public String getContent() {
        return content;
    }

    public Message setContent(String content) {
        this.content = content;
        return this;
    }

    public String getClient() {
        return client;
    }

    public Message setClient(String client) {
        this.client = client;
        return this;
    }

    public Timestamp getCreateTime() {
        return createTime;
    }

    public Message setCreateTime(Timestamp time) {
        this.createTime = new Timestamp(time.getTime());

        //MySQL will lose nanoseconds after saving message
        this.createTime.setNanos(0);

        return this;
    }
}
