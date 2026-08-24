package com.github.overz.config.kafka;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * @param enabled  desliga o provisionamento por completo, para rodar o config-server sem
 *                 Kafka por perto
 * @param location padrao de Resource do Spring com os arquivos de topico
 * @param timeout  quanto esperar por uma chamada do AdminClient
 * @param attempts tentativas ate desistir de um cluster; entre elas espera {@code backoff}
 * @param backoff  espera entre tentativas -- os brokers podem ainda estar subindo
 */
@ConfigurationProperties("lab.kafka.topics")
public record TopicProvisioningProperties(
  Boolean enabled,
  String location,
  Duration timeout,
  Integer attempts,
  Duration backoff
) {

  public TopicProvisioningProperties {
    enabled = enabled == null || enabled;
    location = location == null || location.isBlank() ? "classpath*:kafka/*.yaml" : location;
    timeout = timeout == null ? Duration.ofSeconds(15) : timeout;
    attempts = attempts == null || attempts < 1 ? 30 : attempts;
    backoff = backoff == null ? Duration.ofSeconds(5) : backoff;
  }
}
