package com.github.overz.config.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/**
 * Provisiona os topicos de cada arquivo de {@code resources/kafka} durante o boot.
 *
 * <p>Roda como ApplicationRunner, ou seja depois que a 8888 ja esta escutando: o
 * config-server responde configuracao enquanto isto acontece, e nao ha depends_on nos
 * brokers. Um cluster que ainda esta subindo e esperado com backoff; um que nunca sobe
 * vira erro no log e nada mais. Configuracao nao pode deixar de ser servida porque o
 * Kafka esta fora do ar.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TopicProvisioningRunner implements ApplicationRunner {

  private final TopicCatalogLoader loader;
  private final TopicProvisioner provisioner;
  private final TopicProvisioningProperties options;

  @Override
  public void run(final ApplicationArguments args) {
    if (Boolean.FALSE.equals(options.enabled())) {
      log.info("provisionamento de topicos desligado (lab.kafka.topics.enabled=false)");
      return;
    }

    final var catalogs = loader.load(options.location());

    if (catalogs.isEmpty()) {
      log.warn("nenhum arquivo de topico encontrado em '{}'", options.location());
      return;
    }

    log.info("provisionando topicos, arquivos={}, origem='{}'", catalogs.keySet(), options.location());

    var criados = 0;
    for (final var entry : catalogs.entrySet()) {
      criados += provisioner.provision(entry.getKey(), entry.getValue(), options);
    }

    log.info("provisionamento concluido, dominios={}, criados={}", catalogs.size(), criados);
  }
}
