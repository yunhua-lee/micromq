package io.micromq.operation;

import io.micromq.common.MQException;
import io.micromq.model.Receipt;
import io.micromq.model.Message;

public interface IPull {

    Message pull(long msgId) throws MQException;

    Integer ack(long msgId) throws MQException;

    Receipt getReceipt() throws MQException;

    class IPullErrCode {
        public static final int ACK_SUCCESS = 0;
        public static final int ACK_REPEATED = 1;
        public static final int ACK_DISORDERED = 2;
        public static final int ACK_TIMEOUT = 3;
    }
}
