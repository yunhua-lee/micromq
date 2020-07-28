package io.micromq.operation.strategy.pull;

import io.micromq.common.MQCode;
import io.micromq.common.MQException;
import io.micromq.model.Message;
import io.micromq.model.Receipt;
import io.micromq.operation.IPull;

public class NoPullPrivilege implements IPull {

    @Override
    public Message pull(long msgId) throws MQException {
        throw new MQException(MQCode.NO_PRIVILEGE, "no pull privilege");
    }

    @Override
    public Integer ack(long msgId) throws MQException {
        throw new MQException(MQCode.NO_PRIVILEGE, "no pull privilege");
    }

    @Override
    public Receipt getReceipt() throws MQException {
        return null;
    }
}
