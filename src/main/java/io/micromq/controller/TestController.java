package io.micromq.controller;

import io.micromq.common.MQException;
import io.micromq.config.ClientManager;
import io.micromq.config.ServerConfig;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class TestController {

    @RequestMapping(value = {"/", "/test"})
    public String test(Model model) throws MQException {
        String serverName = ServerConfig.INSTANCE().getServerName();
        String adminSign = ClientManager.INSTANCE().getClient("admin").getSign();
        String testSign = ClientManager.INSTANCE().getClient("test").getSign();

        model.addAttribute("serverName", serverName);
        model.addAttribute("adminSign", adminSign);
        model.addAttribute("testSign", testSign);

        return "test";
    }
}
