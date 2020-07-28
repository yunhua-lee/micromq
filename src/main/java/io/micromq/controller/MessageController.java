package io.micromq.controller;

import com.google.gson.JsonObject;
import io.micromq.client.MQClient;
import io.micromq.common.MQCode;
import io.micromq.common.MQException;
import io.micromq.config.ClientManager;
import io.micromq.config.QueueManager;
import io.micromq.controller.response.*;
import io.micromq.model.Message;
import io.micromq.log.MQLog;
import io.micromq.operation.MQOperation;
import io.micromq.server.MQNode;
import io.micromq.util.SpringUtils;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.sql.Timestamp;

@RestController
public class MessageController {
    private static final Logger logger = LoggerFactory.getLogger(MessageController.class);

    @RequestMapping(value = "/queues/{queueName}/messages/{msgId}", method = RequestMethod.GET)
    public @ResponseBody PullResponse pull(HttpServletRequest request,
                                           @RequestParam("requestId") Long requestId,
                                           @RequestParam("client") String clientName,
                                           @PathVariable("queueName") String queueName,
                                           @RequestParam("sign") String sign,
                                           @PathVariable Long msgId) {
        String url = SpringUtils.getRealURL(request);
        clientName = StringUtils.trim(clientName);
        queueName = StringUtils.trim(queueName);
        sign = StringUtils.trim(sign);

        long begin = System.currentTimeMillis();

        PullResponse response = new PullResponse();
        response.setClient(clientName);
        response.setRequestId(requestId);

        try {
            if( !checkParam(requestId, clientName, queueName, response)){
                return response;
            }

            if (!MQNode.INSTANCE().supportPull()) {
                response.setRetCode(MQCode.UNSUPPORTED);
                response.setRetMsg("MQNode " + MQNode.INSTANCE().getName() + " doesn't support pull operation now");

                return response;
            }

            if (!QueueManager.INSTANCE().getQueue(queueName).isActive()) {
                response.setRetCode(MQCode.INVALID_QUEUE);
                response.setRetMsg("queue is not active: " + queueName);

                return response;
            }

            MQClient client = ClientManager.INSTANCE().getClient(clientName);

            if (!client.isActive()) {
                response.setRetCode(MQCode.INVALID_CLIENT);
                response.setRetMsg("client isn't active: " + client);

                return response;
            }

            if(!client.checkSign(sign)){
                response.setRetCode(MQCode.INVALID_SIGN);
                response.setRetMsg("sign check failed, client sign: " + sign);
                return response;
            }

            MQOperation operation = client.getOperation(queueName);
            Message message = operation.pull(msgId);

            if(message != null) {
                response.setRetCode(MQCode.SUCCESS);
                response.setRetMsg("success");
                response.setMessage(new PullMessage(message));
            }
            else {
                response.setRetCode(MQCode.NO_DATA);
                response.setRetMsg("no data");
                response.setMessage(null);
            }

            return response;
        }
        catch (MQException e){
            response.setRetCode(e.getErrCode());
            response.setRetMsg(e.getMessage());

            logger.error(e.getMessage(), e);

            return response;
        }
        catch (Throwable t){
            response.setRetCode(MQCode.EXCEPTION);
            response.setRetMsg(t.getMessage());

            logger.error(t.getMessage(), t);

            return response;
        }
        finally {
            long end = System.currentTimeMillis();
            long elapse = end - begin;
            logResponse(url, elapse, response);
        }
    }

    @RequestMapping(value = "/queues/{queueName}/messages/{msgId}", method = RequestMethod.PUT)
    public AckResponse ack(HttpServletRequest request,
                           @RequestParam(required = false, defaultValue = "0") Long requestId,
                           @RequestParam(required = false, defaultValue = "") String clientName,
                           @PathVariable(name = "queueName") String queueName,
                           @RequestParam(required = false, defaultValue = "") String sign,
                           @PathVariable Long msgId,
                           @RequestParam(required = false, defaultValue = "") String status) {
        String url = SpringUtils.getRealURL(request);
        clientName = StringUtils.trim(clientName);
        queueName = StringUtils.trim(queueName);
        sign = StringUtils.trim(sign);

        long begin = System.currentTimeMillis();

        AckResponse response = new AckResponse();
        response.setClient(clientName);
        response.setRequestId(requestId);

        try {
            if( !checkParam(requestId, clientName, queueName, response)){
                return response;
            }

            if (status == null || !status.equalsIgnoreCase("ack")) {
                response.setRetCode(MQCode.INVALID_REQUEST);
                response.setRetMsg("'status' must be 'ack'");

                return response;
            }

            if (msgId == null || msgId < 1) {
                response.setRetCode(MQCode.INVALID_REQUEST);
                response.setRetMsg("'msgId' can not be null or less than 1: " + msgId);

                return response;
            }

            if (!MQNode.INSTANCE().supportPull()) {
                response.setRetCode(MQCode.UNSUPPORTED);
                response.setRetMsg("MQNode " + MQNode.INSTANCE().getName() + " doesn't support ack operation now");

                return response;
            }

            if (!QueueManager.INSTANCE().getQueue(queueName).isActive()) {
                response.setRetCode(MQCode.INVALID_QUEUE);
                response.setRetMsg("queue is not active: " + queueName);

                return response;
            }

            MQClient client = ClientManager.INSTANCE().getClient(clientName);

            if (!client.isActive()) {
                response.setRetCode(MQCode.INVALID_CLIENT);
                response.setRetMsg("client isn't active: " + client);

                return response;
            }

            if(!client.checkSign(sign)){
                response.setRetCode(MQCode.INVALID_SIGN);
                response.setRetMsg("sign check failed, client sign: " + sign);
                return response;
            }

            MQOperation queue = client.getOperation(queueName);
            Integer result = queue.ack(msgId);

            response = AckResponse.build(msgId, result, response);

            return response;
        }
        catch (Throwable t){
            response.setRetCode(MQCode.EXCEPTION);
            response.setRetMsg(t.getMessage());

            logger.error(t.getMessage(), t);

            return response;
        }
        finally {
            long end = System.currentTimeMillis();
            long elapse = end - begin;
            logResponse(url, elapse, response);
        }
    }

    @RequestMapping(value = "/queues/{queueName}/messages", method = RequestMethod.POST,
                    consumes = "application/json")
    public PublishResponse publish(HttpServletRequest request,
                                   @PathVariable String queueName,
                                   @RequestBody JsonObject body) throws JSONException {
        String url = SpringUtils.getRealURL(request);
        queueName = StringUtils.trim(queueName);

        Long requestId = body.get("requestId").getAsLong();
        String clientName = StringUtils.trim(body.get("client").getAsString());
        String sign = StringUtils.trim(body.get("sign").getAsString());
        String content = body.get("content").getAsString();

        long begin = System.currentTimeMillis();

        PublishResponse response = new PublishResponse();
        response.setClient(clientName);
        response.setRequestId(requestId);

        try {
            if( !checkParam(requestId, clientName, queueName, response)){
                return response;
            }

            if (!MQNode.INSTANCE().supportPublish()) {
                response.setRetCode(MQCode.UNSUPPORTED);
                response.setRetMsg("MQNode " + MQNode.INSTANCE().getName() + " doesn't support publish operation now");

                return response;
            }

            if (!QueueManager.INSTANCE().getQueue(queueName).isActive()) {
                response.setRetCode(MQCode.INVALID_QUEUE);
                response.setRetMsg("queue is not active: " + queueName);

                return response;
            }

            MQClient client = ClientManager.INSTANCE().getClient(clientName);
            if (!client.isActive()) {
                response.setRetCode(MQCode.INVALID_CLIENT);
                response.setRetMsg("client isn't active: " + client);

                return response;
            }

            if(!client.checkSign(sign)){
                response.setRetCode(MQCode.INVALID_SIGN);
                response.setRetMsg("sign check failed, client sign: " + sign);
                return response;
            }

            Message msg = new Message();
            msg.setQueue(queueName);
            msg.setContent(content);
            msg.setClient(clientName);
            msg.setCreateTime(new Timestamp(System.currentTimeMillis()));

            MQOperation operation = client.getOperation(queueName);
            Boolean result = operation.save(msg);

            if(result) {
                response.setRetCode(MQCode.SUCCESS);
                response.setRetMsg("success");
            }
            else {
                response.setRetCode(MQCode.FAILED);
                response.setRetMsg("failed");
            }
            response.setRequestId(requestId);
            response.setClient(clientName);

            return response;

        }catch (Throwable t){
            response.setRetCode(MQCode.EXCEPTION);
            response.setRetMsg(t.getMessage());

            logger.error(t.getMessage(), t);

            return response;
        }
        finally {
            long end = System.currentTimeMillis();
            long elapse = end - begin;
            logResponse(url, elapse, response);
        }
    }

    private boolean checkParam(Long requestId, String client, String queue, MQResponse response){
        if (client == null || client.equals("")) {
            response.setRetCode(MQCode.INVALID_REQUEST);
            response.setRetMsg("'client' can not be null or empty");

            return false;
        }

        if (requestId == null || requestId < 1) {
            response.setRetCode(MQCode.INVALID_REQUEST);
            response.setRetMsg("'requestId' can not be less than 1");

            return false;
        }

        return true;
    }

    private void logResponse(String url, long elapse, MQResponse response){
        MQLog log = new MQLog("request").p("url", url).p("elapse(ms)", elapse);
        logger.info(log.toString());

        log.setMessage("request finished");
        log.p("requestId", response.getRequestId());
        log.p("client", response.getClient());
        log.p("retCode", response.getRetCode());
        log.p("retMsg", response.getRetMsg());

        if( !response.getRetCode().equals(MQCode.SUCCESS)){
            logger.error(log.toString());
        }else{
            logger.info(log.toString());
        }
    }
}
