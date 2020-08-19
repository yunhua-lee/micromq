package io.micromq.server;

import io.micromq.config.*;
import io.micromq.config.impl.FileConfigSource;
import io.micromq.config.impl.MQAdminConfigSource;
import io.micromq.util.SysUtils;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
public class MQRunner implements ApplicationRunner{
    private static final Logger logger = LoggerFactory.getLogger(MQRunner.class);

    @Override
    public void run(ApplicationArguments applicationArguments) throws Exception {
        String file = SysUtils.getAbsolutePath("config/server.properties");

        logger.info("start to load server config file: " + file);

        PropertiesConfiguration configuration = new PropertiesConfiguration(file);
        ServerConfig.build(configuration);

        String type = ServerConfig.INSTANCE().getServerRole();
        ConfigSource configSource;
        if(ServerConfig.SINGLE_SERVER.equalsIgnoreCase(type)){
            //load config from local file
            configSource = new FileConfigSource();
        }
        else {
            //load config from MQAdmin
            configSource = new MQAdminConfigSource(ServerConfig.INSTANCE().getAdminIP(),
                    ServerConfig.INSTANCE().getAdminPort());
        }

        logger.info("server type is: " + type);

        ClientManager.init(configSource);
        configSource.addListener(ClientManager.CONFIG_TYPE, ClientManager.INSTANCE());

        PrivilegeConfig.init(configSource);
        configSource.addListener(PrivilegeConfig.CONFIG_TYPE, PrivilegeConfig.INSTANCE());

        QueueManager.init(configSource);
        configSource.addListener(QueueManager.CONFIG_TYPE, QueueManager.INSTANCE());

        RuntimeConfig.init(configSource);
        configSource.addListener(RuntimeConfig.CONFIG_TYPE, RuntimeConfig.INSTANCE());

        logger.info("finished to load all config");

        MQNode node = MQNode.build(configSource);
        configSource.addListener(MQNode.CONFIG_TYPE, MQNode.INSTANCE());

        node.start();

        logger.info("node start:" + node.getName());
    }
}
