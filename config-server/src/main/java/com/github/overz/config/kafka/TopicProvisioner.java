package com.github.overz.config.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Cria os topicos de um catalogo. So cria.
 *
 * <p>Topico que ja existe e pulado -- nao ha comparacao de particoes nem de retencao, e
 * nada e alterado ou apagado. E deliberado: mudar particoes de um topico vivo remapeia as
 * chaves entre elas e quebra a ordenacao por chave que o Kafka Streams depende, e reduzir
 * retencao apaga dado. Essas operacoes existem, mas nao devem acontecer sozinhas no boot
 * de um servidor de configuracao.
 *
 * <p>Falha nao derruba o config-server: servir configuracao nao pode depender de o Kafka
 * estar de pe. O que da errado vai para o log com o nome do topico e a causa.
 */
@Slf4j
@Component
public class TopicProvisioner {

  /**
   * @return quantos topicos foram criados nesta chamada
   */
  public int provision(final String domain, final TopicCatalog catalog, final TopicProvisioningProperties options) {
    final var desired = catalog.newTopics();

    if (desired.isEmpty()) {
      log.info("nenhum topico declarado, dominio='{}'", domain);
      return 0;
    }

    try (final var admin = Admin.create(adminConfig(catalog, options))) {
      final var existing = awaitCluster(domain, admin, options);

      final var missing = desired.stream()
        .filter(topic -> !existing.contains(topic.name()))
        .toList();

      final var jaExistem = desired.size() - missing.size();

      if (missing.isEmpty()) {
        log.info("topicos ja existentes, nada a criar, dominio='{}', declarados={}", domain, desired.size());
        return 0;
      }

      return create(domain, admin, missing, jaExistem, options);
    } catch (final RuntimeException e) {
      log.error("provisionamento falhou, dominio='{}', brokers='{}', causa='{}'",
        domain, catalog.bootstrapServers(), e.toString());
      return 0;
    }
  }

  private int create(final String domain,
                     final Admin admin,
                     final List<NewTopic> missing,
                     final int jaExistem,
                     final TopicProvisioningProperties options) {

    final var result = admin.createTopics(missing);
    var criados = 0;
    var falharam = 0;

    for (final var entry : result.values().entrySet()) {
      final var name = entry.getKey();

      try {
        entry.getValue().get(options.timeout().toMillis(), TimeUnit.MILLISECONDS);
        criados++;
        log.info("  criado  {}", describe(missing, name));
      } catch (final ExecutionException e) {
        // Corrida com outro provisionador, ou com o MirrorMaker 2: alguem criou entre o
        // listTopics e o createTopics. Tratado como sucesso -- o topico existe.
        if (e.getCause() instanceof TopicExistsException) {
          log.info("  existe  {} (criado entre a listagem e a criacao)", name);
        } else {
          falharam++;
          log.error("  FALHOU  {} -- {}", name, e.getCause() == null ? e.toString() : e.getCause().getMessage());
        }
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        log.warn("provisionamento interrompido, dominio='{}'", domain);
        return criados;
      } catch (final Exception e) {
        falharam++;
        log.error("  FALHOU  {} -- {}", name, e.toString());
      }
    }

    log.info("topicos provisionados, dominio='{}', criados={}, jaExistiam={}, falharam={}",
      domain, criados, jaExistem, falharam);

    return criados;
  }

  /**
   * Espera o cluster responder e devolve os topicos que ele ja tem. Os brokers podem
   * ainda estar subindo: o config-server nao declara depends_on neles de proposito, para
   * continuar servindo configuracao mesmo com o Kafka fora do ar.
   */
  private Set<String> awaitCluster(final String domain,
                                   final Admin admin,
                                   final TopicProvisioningProperties options) {

    RuntimeException last = null;

    for (var attempt = 1; attempt <= options.attempts(); attempt++) {
      try {
        return admin.listTopics().names().get(options.timeout().toMillis(), TimeUnit.MILLISECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("interrompido esperando o cluster de '%s'".formatted(domain), e);
      } catch (final Exception e) {
        last = new IllegalStateException(
          "cluster de '%s' nao respondeu em %d tentativa(s)".formatted(domain, options.attempts()), e);

        if (attempt < options.attempts()) {
          log.info("aguardando o cluster, dominio='{}', tentativa={}/{}", domain, attempt, options.attempts());
          sleep(options);
        }
      }
    }

    throw last;
  }

  private void sleep(final TopicProvisioningProperties options) {
    try {
      Thread.sleep(options.backoff().toMillis());
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrompido durante o backoff", e);
    }
  }

  private static Map<String, Object> adminConfig(final TopicCatalog catalog,
                                                 final TopicProvisioningProperties options) {
    final var timeout = (int) options.timeout().toMillis();

    return Map.of(
      AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, catalog.bootstrapServers(),
      AdminClientConfig.CLIENT_ID_CONFIG, "config-server-topic-provisioner",
      AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, timeout,
      AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, timeout
    );
  }

  private static String describe(final List<NewTopic> topics, final String name) {
    return topics.stream()
      .filter(topic -> topic.name().equals(name))
      .findFirst()
      .map(topic -> "%s  particoes=%d rf=%d configs=%s"
        .formatted(topic.name(), topic.numPartitions(), topic.replicationFactor(), topic.configs()))
      .orElse(name);
  }
}
