package io.micromq.dao.impl.mysql;

import io.micromq.dao.IReceiptDao;
import io.micromq.model.Receipt;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class MySQLReceiptDao implements IReceiptDao {
    private static final String table = "Receipt";

    private final JdbcTemplate jdbcTemplate;

    public MySQLReceiptDao(DriverManagerDataSource dataSource){
        jdbcTemplate = new JdbcTemplate(dataSource);
    }

    private static final RowMapper<Receipt> mapper = new RowMapper<Receipt>() {
        @Override
        public Receipt mapRow(ResultSet resultSet, int i) throws SQLException {
            Receipt receipt = new Receipt();

            receipt.setClient(resultSet.getString("client"));
            receipt.setQueue(resultSet.getString("queue"));
            receipt.setMsgId(resultSet.getLong("msgId"));
            receipt.setTime(resultSet.getTimestamp("updateTime"));

            return receipt;
        }
    };

    @Override
    public Receipt getReceipt(String client, String queue) {
        String sql = "SELECT * FROM " + table + " WHERE client=? and queue=?";
        try {
            return jdbcTemplate.queryForObject(sql, mapper, client, queue);
        }
        catch (EmptyResultDataAccessException e){
            return null;
        }
    }

    @Override
    public Integer updateReceipt(Receipt receipt) {
        String sql = "INSERT INTO " + table + "(client, queue, msgId, updateTime) VALUES(?, ?, ?, ?) " +
                "ON DUPLICATE KEY UPDATE msgId=?, updateTime=?";
        return jdbcTemplate.update(sql, receipt.getClient(), receipt.getQueue(), receipt.getMsgId(), receipt.getTime(),
                receipt.getMsgId(), receipt.getTime());
    }

    @Override
    public Integer updateReceiptList(final List<Receipt> receipts) {
        Integer result = 0;
        for(Receipt receipt : receipts){
            result += updateReceipt(receipt);
        }

        return result;
    }
}
