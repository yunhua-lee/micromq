package io.micromq.operation.strategy.save;

import io.micromq.common.MQCode;
import io.micromq.common.MQException;
import io.micromq.model.Message;
import io.micromq.operation.ISave;

public class NoPublishPrivilege implements ISave {
    @Override
    public Boolean save(Message message) throws MQException {
        throw new MQException(MQCode.NO_PRIVILEGE, "no publish privilege");
    }
}
