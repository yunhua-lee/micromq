package io.micromq.config;

import org.apache.commons.configuration.Configuration;

public interface ConfigSource {
    Configuration getConfig(String type);
    void addListener(String type, Listener listener);

    interface Listener {
        void refresh(ConfigSource source);
    }
}
