package io.micromq.controller.response;

public class PullResponse extends MQResponse{

    private PullMessage message;

    public PullMessage getMessage() {
        return message;
    }

    public PullResponse setMessage(PullMessage message) {
        this.message = message;
        return this;
    }
}
