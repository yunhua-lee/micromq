package io.micromq.client;

import io.micromq.config.QueueManager;
import io.micromq.log.MQLog;
import io.micromq.operation.MQOperation;
import io.micromq.queue.MQQueue;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.DigestUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MQClient {
    private static final Logger logger = LoggerFactory.getLogger(MQClient.class);

    private final String clientName;
    private volatile boolean active;
    private volatile String signKey;

    private final Map<String, MQOperation> operationMap = new ConcurrentHashMap<>();

    public MQClient(String name){
        this.clientName = name;
    }

    public String getClientName() {
        return clientName;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public void setSignKey(String signKey) {
        Validate.notEmpty(signKey);

        this.signKey = signKey;
    }

    public String getSign(){
        String serverSign = DigestUtils.md5DigestAsHex((clientName + "." + signKey).getBytes());

        return serverSign;
    }

    public Boolean checkSign(String sign){
        String serverSign = getSign();
        Boolean result =  serverSign.equalsIgnoreCase(sign);
        if(!result){
            MQLog log = new MQLog("sign check failed").p("serverSign", serverSign).p("clientSign", sign);
            logger.warn(log.toString());
        }

        return result;
    }

    public MQOperation getOperation(String queueName){
        MQOperation operation = operationMap.get(queueName);
        if(operation == null){
            MQQueue queue = QueueManager.INSTANCE().getQueue(queueName);
            operation = MQOperation.build(this, queue);
            MQOperation old = operationMap.putIfAbsent(queueName, operation);
            if( old != null ){
                operation = old;
            }
        }

        return operation;
    }

    public List<MQOperation> getAllOperations(){
        return new ArrayList<>(operationMap.values());
    }
}
