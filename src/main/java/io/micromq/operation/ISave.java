package io.micromq.operation;

import io.micromq.common.MQException;
import io.micromq.model.Message;

public interface ISave {
    Boolean save(Message message) throws MQException;
}
