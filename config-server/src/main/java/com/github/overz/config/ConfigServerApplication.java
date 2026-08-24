package com.github.overz.config;

import com.github.overz.config.kafka.TopicProvisioningProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.config.server.EnableConfigServer;

@EnableConfigServer
@SpringBootApplication
@EnableConfigurationProperties(TopicProvisioningProperties.class)
public class ConfigServerApplication {

  static void main(final String[] args) {
    SpringApplication.run(ConfigServerApplication.class, args);
  }
}
