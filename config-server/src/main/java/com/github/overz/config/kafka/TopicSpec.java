package com.github.overz.config.kafka;

import org.apache.kafka.clients.admin.NewTopic;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Um topico como declarado no YAML.
 *
 * <p>Particoes e fator de replicacao sao campos proprios porque o Kafka os trata assim --
 * vao no construtor de {@link NewTopic}, nao no mapa de configuracao. Todo o resto entra
 * em {@code configs} como texto, exatamente o que {@code kafka-topics.sh --config} aceita.
 * Um mapa aberto em vez de quarenta campos tipados: a lista de propriedades de topico
 * muda a cada versao do Kafka, e quem valida chave desconhecida e o broker.
 *
 * @param name              nome do topico, obrigatorio
 * @param partitions        sobrepoe o default do arquivo
 * @param replicationFactor sobrepoe o default do arquivo
 * @param configs           mesclado sobre o default do arquivo, chave a chave
 */
public record TopicSpec(
  String name,
  Integer partitions,
  Short replicationFactor,
  Map<String, String> configs
) {

  public TopicSpec {
    if (name == null || name.isBlank()) {
      throw new IllegalArgumentException("topico sem 'name' no yaml");
    }

    configs = configs == null ? Map.of() : Map.copyOf(configs);
  }

  /**
   * Resolve os valores contra os defaults do arquivo. O merge de {@code configs} e por
   * chave, e nao tudo-ou-nada: um topico que declara so retention.ms continua herdando o
   * min.insync.replicas do default.
   */
  NewTopic toNewTopic(final TopicDefaults defaults) {
    final var merged = new LinkedHashMap<>(defaults.configs());
    merged.putAll(configs);

    final var topic = new NewTopic(
      name,
      partitions == null ? defaults.partitions() : partitions,
      replicationFactor == null ? defaults.replicationFactor() : replicationFactor
    );

    return topic.configs(merged);
  }
}
