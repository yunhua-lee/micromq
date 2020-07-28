package io.micromq;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.security.SecurityAutoConfiguration;

@SpringBootApplication(exclude = {SecurityAutoConfiguration.class})
public class MQBootstrap {
    public static void main(String[] args) throws Exception {
        SpringApplication.run(MQBootstrap.class, args);
    }
}
