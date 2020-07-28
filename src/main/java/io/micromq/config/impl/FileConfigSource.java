package io.micromq.config.impl;

import io.micromq.config.ConfigSource;
import io.micromq.util.SysUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.reloading.FileChangedReloadingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FileConfigSource implements ConfigSource {
    private static final Logger logger = LoggerFactory.getLogger(FileConfigSource.class);

    private final Map<String, List<ConfigSource.Listener>> listenerMap = new HashMap<>();
    private final Map<String, PropertiesConfiguration> configMap = new ConcurrentHashMap<>();

    @Override
    public Configuration getConfig(String type) {
        PropertiesConfiguration configuration = configMap.get(type);
        if(configuration == null){
            loadConfig(type);
            configuration = configMap.get(type);
        }
        return configuration;
    }

    @Override
    public void addListener(String type, Listener listener) {
        List<ConfigSource.Listener> listenerList = listenerMap.get(type);
        if(listenerList == null){
            listenerList = new ArrayList<>();
            listenerMap.put(type, listenerList);
        }
        listenerList.add(listener);
    }

    private void loadConfig(String type){
        String filePath = buildPath(type);
        PropertiesConfiguration configuration;
        try {
            configuration = new PropertiesConfiguration(filePath);

            //Refresh configuration per 5 seconds
            FileChangedReloadingStrategy strategy = new FileChangedReloadingStrategy();
            strategy.setRefreshDelay(5 * 1000);
            configuration.setReloadingStrategy(strategy);

        } catch (ConfigurationException e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }

        configMap.putIfAbsent(type, configuration);
    }

    private String buildPath(String type){
        String fileName = type + ".properties";
        return SysUtils.getAbsolutePath("config/" + fileName);
    }
}
