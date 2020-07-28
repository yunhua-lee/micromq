package io.micromq.dao.impl.mysql;

import io.micromq.dao.IMessageDao;
import io.micromq.model.Message;
import org.owasp.esapi.ESAPI;
import org.owasp.esapi.Encoder;
import org.owasp.esapi.codecs.Codec;
import org.owasp.esapi.codecs.MySQLCodec;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

public class MySQLMessageDao implements IMessageDao {

    private final JdbcTemplate jdbcTemplate;

    private static final Codec codec = new MySQLCodec(MySQLCodec.Mode.ANSI);
    private static final Encoder encoder = ESAPI.encoder();

    public MySQLMessageDao(DriverManagerDataSource dataSource){
        jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Override
    public Message getMessage(final String queue, long id) {
        String sql = "SELECT * FROM " + queue + " WHERE msgId = ?";

        try {
            return jdbcTemplate.queryForObject(sql, getRowMapper(queue), id);
        }
        catch (EmptyResultDataAccessException e){
            return null;
        }
    }

    @Override
    public List<Message> getMessageList(String queue, long beginMsgId, int count) {
        String sql = "SELECT * FROM " + queue +" WHERE msgId >= ? limit ?";
        return jdbcTemplate.query(sql, getRowMapper(queue), beginMsgId, count);
    }

    @Override
    public Message getFirstMessage(String queue) {
        String sql = "SELECT * FROM " + queue +" ORDER BY msgId ASC LIMIT 1";

        try {
            return jdbcTemplate.queryForObject(sql, getRowMapper(queue));
        }
        catch (EmptyResultDataAccessException e){
            return null;
        }
    }

    @Override
    public Message getLastMessage(String queue) {
        String sql = "SELECT * FROM "+ queue +" ORDER BY msgId DESC LIMIT 1";

        try {
            return jdbcTemplate.queryForObject(sql, getRowMapper(queue));
        }
        catch (EmptyResultDataAccessException e){
            return null;
        }
    }

    @Override
    public Integer saveMessage(Message message) {
        String sql = "INSERT INTO " + message.getQueue() + "(content, client, createTime) " + "VALUES(?, ?, ?)";
        return jdbcTemplate.update(sql, message.getContent(), message.getClient(), message.getCreateTime());
    }

    @Override
    public Integer saveMessages(List<Message> messages) {
        String[] sqlList = new String[messages.size()];
        int pos = 0;

        for(Message msg : messages){
            if(msg == null){
                continue;
            }
            StringBuilder builder = new StringBuilder();

            builder.append("INSERT INTO ");
            builder.append(msg.getQueue());
            builder.append("(content, client, createTime) VALUES");
            builder.append("(");
            builder.append("'" + encoder.encodeForSQL(codec, msg.getContent()) + "'");
            builder.append(",");
            builder.append("'" + encoder.encodeForSQL(codec, msg.getClient()) + "'");
            builder.append(",");
            builder.append("'" + msg.getCreateTime() + "'");
            builder.append(")");

            sqlList[pos++] = builder.toString();
        }

        int[] result = jdbcTemplate.batchUpdate(sqlList);
        Integer total = 0;

        for(int i = 0; i < result.length; i++){
            total += result[i];
        }

        return total;
    }

    @Override
    public Integer deleteMessages(String queue, Timestamp until, int limit) {
        String sql;
        Message lastMessage = getLastMessage(queue);
        if( lastMessage == null ){
            return 0;
        }
        else{
            sql = "DELETE FROM " + queue + " WHERE createTime < ? and msgId < ? limit ?";
            return jdbcTemplate.update(sql, until, lastMessage.getMsgId(), limit);
        }
    }

    private RowMapper<Message> getRowMapper(final String queue){
        return new RowMapper<Message>() {
            @Override
            public Message mapRow(ResultSet resultSet, int i) throws SQLException {
                Message msg = new Message();
                msg.setQueue(queue);
                msg.setMsgId(resultSet.getLong("msgId"));
                msg.setClient(resultSet.getString("client"));
                msg.setContent(resultSet.getString("content"));
                msg.setCreateTime(resultSet.getTimestamp("createTime"));

                return msg;
            }
        };
    }
}
