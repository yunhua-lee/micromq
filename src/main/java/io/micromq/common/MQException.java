package io.micromq.common;

public class MQException extends Exception{
    private final String errCode;

    public String getErrCode() {
        return errCode;
    }

    public MQException(String errCode, String message){
        super(message);
        this.errCode = errCode;
    }
}
