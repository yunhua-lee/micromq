package io.micromq.dao;

import io.micromq.model.Message;

import java.sql.Timestamp;
import java.util.List;

public interface IMessageDao {
    /**
     * Get message by id
     * @param queue
     * @param id
     * @return
     */
    Message getMessage(final String queue, long id);

    /**
     * Get message list
     *
     * @param queue
     * @param beginMsgId
     * @param count
     * @return
     */
    List<Message> getMessageList(String queue, long beginMsgId, int count);

    /**
     * Get first message which is the oldest message
     * @param queue
     * @return
     */
    Message getFirstMessage(String queue);

    /**
     * Get last message which is the newest message
     * @param queue
     * @return
     */
    Message getLastMessage(String queue);

    /**
     * Save single message
     * @param message
     * @return
     */
    Integer saveMessage(Message message);

    /**
     * Save multi messages which belong to different queue
     * @param messages
     * @return
     */
    Integer saveMessages(List<Message> messages);

    Integer deleteMessages(String queue, Timestamp until, int limit);
}
