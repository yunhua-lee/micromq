package io.micromq.controller.response;

import io.micromq.common.MQCode;
import io.micromq.operation.IPull;

public class AckResponse extends MQResponse{
    public static final String ACK_REPEATED = "ACK_REPEATED";
    public static final String ACK_DISORDERED = "ACK_DISORDERED";
    public static final String ACK_TIMEOUT = "ACK_TIMEOUT";

    public static AckResponse build(Long msgId, Integer ackResult, AckResponse response){
        switch (ackResult){
            case IPull.IPullErrCode.ACK_SUCCESS:
                response.setRetCode(MQCode.SUCCESS);
                response.setRetMsg("success");
            case IPull.IPullErrCode.ACK_REPEATED:
                response.setRetCode(AckResponse.ACK_REPEATED);
                response.setRetMsg("repeated ack for message: " + msgId);
            case IPull.IPullErrCode.ACK_DISORDERED:
                response.setRetCode(AckResponse.ACK_DISORDERED);
                response.setRetMsg("disordered ack for message: " + msgId);
            case IPull.IPullErrCode.ACK_TIMEOUT:
                response.setRetCode(AckResponse.ACK_TIMEOUT);
                response.setRetMsg("timeout ack for message: " + msgId);
            default:
                response.setRetCode(MQCode.EXCEPTION);
                response.setRetMsg("unknown exception, errcode is: " + ackResult);
        }

        return response;
    }
}
