package io.micromq.controller;

import io.micromq.config.ServerConfig;
import io.micromq.controller.response.StatusResponse;
import io.micromq.server.MQNode;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class StatusController {

    @RequestMapping(value = "/status", method = RequestMethod.GET)
    public @ResponseBody StatusResponse getStatus(){
        StatusResponse response = new StatusResponse();
        MQNode node = MQNode.INSTANCE();

        response.setServerName(ServerConfig.INSTANCE().getServerName());
        response.setRole(ServerConfig.INSTANCE().getServerRole());
        response.setActive(node.isActive());
        response.setSupportPull(node.supportPull());
        response.setSupportPublish(node.supportPublish());

        return response;
    }
}
