package io.micromq.controller;

import io.micromq.common.MQCode;
import io.micromq.controller.response.AdminResponse;
import io.micromq.server.MQNode;
import org.springframework.web.bind.annotation.*;

@RestController
public class AdminController {

    @RequestMapping(value = "/admin", method = RequestMethod.GET)
    public @ResponseBody AdminResponse admin(@RequestParam(required = false, defaultValue = "") String client,
            @RequestParam(required = false, defaultValue = "0") Long requestId,
            @RequestParam(required = false, defaultValue = "") String sign,
            @RequestParam(required = false, defaultValue = "") String operation){
        AdminResponse response = new AdminResponse();
        response.setRequestId(requestId);
        response.setClient(client);

        if (client == null || client.equals("")) {
            response.setRetCode(MQCode.INVALID_REQUEST);
            response.setRetMsg("'client' can not be null or empty");

            return response;
        }

        if(!client.equalsIgnoreCase("admin")){
            response.setRetCode(MQCode.INVALID_CLIENT);
            response.setRetMsg("only 'admin' can perform administration operation");

            return response;
        }

        if (requestId == null || requestId < 1) {
            response.setRetCode(MQCode.INVALID_REQUEST);
            response.setRetMsg("'requestId' can not be less than 1");

            return response;
        }

        if (sign == null || sign.equals("")) {
            response.setRetCode(MQCode.INVALID_REQUEST);
            response.setRetMsg("'sign' can not be null or empty");

            return response;
        }

        if(operation.equalsIgnoreCase("start")){
            MQNode.INSTANCE().start();
            response.setRetCode(MQCode.SUCCESS);
            response.setRetMsg("OK");
        }else if(operation.equalsIgnoreCase("stop")){
            MQNode.INSTANCE().stop();
            response.setRetCode(MQCode.SUCCESS);
            response.setRetMsg("OK");
        }else {
            response.setRetCode(MQCode.FAILED);
            response.setRetMsg("unknown operation: " + operation);
        }

        return response;
    }
}
