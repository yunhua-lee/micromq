package io.micromq.operation.strategy.save;

import io.micromq.common.MQException;
import io.micromq.dao.IMessageDao;
import io.micromq.model.Message;
import io.micromq.operation.ISave;
import io.micromq.server.MQNode;

public class SyncSave implements ISave {

    private final IMessageDao messageDao;

    private SyncSave(){
        this.messageDao = MQNode.INSTANCE().getMessageDao();
    }

    public static SyncSave build(){
        return new SyncSave();
    }

    @Override
    public Boolean save(Message message) throws MQException {
        int result = messageDao.saveMessage(message);
        return result == 1;
    }
}
