package io.micromq.common;

/**
 * @Desctiption
 * @Author wallace
 * @Date 2020/8/18
 */
public class MQRuntimeException extends RuntimeException{
	private final String errCode;

	public String getErrCode() {
		return errCode;
	}

	public MQRuntimeException(String errCode, String message){
		super(message);
		this.errCode = errCode;
	}
}
