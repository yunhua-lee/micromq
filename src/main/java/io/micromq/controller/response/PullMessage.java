package io.micromq.controller.response;

import io.micromq.config.ServerConfig;
import io.micromq.model.Message;
import io.micromq.util.SysUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Timestamp;

public class PullMessage{
    private String msgId;
    private String content;
    private String client;

    private Timestamp time;

    public PullMessage(Message message){
        this.msgId = genUniqueMsgId(message.getMsgId());
        this.content = message.getContent();
        this.client = message.getClient();
        this.time = message.getCreateTime();
    }

    private static String genUniqueMsgId(long msgId){
        String serverName = ServerConfig.INSTANCE().getServerName();
        if(StringUtils.isBlank(serverName)){
            serverName = SysUtils.getHostName() + "_" + ServerConfig.INSTANCE().getPort();
        }
        return serverName + "_" + msgId;
    }
}
