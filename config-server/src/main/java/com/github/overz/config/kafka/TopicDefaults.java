package com.github.overz.config.kafka;

import java.util.Map;

/**
 * Valores aplicados a todo topico do arquivo que nao declarar os proprios. Existe para que
 * acrescentar um topico ao YAML seja uma linha, e nao um bloco repetindo particoes, fator
 * de replicacao e min.insync.replicas.
 */
public record TopicDefaults(
  Integer partitions,
  Short replicationFactor,
  Map<String, String> configs
) {

  private static final int DEFAULT_PARTITIONS = 3;
  private static final short DEFAULT_REPLICATION_FACTOR = 3;

  public TopicDefaults {
    partitions = partitions == null ? DEFAULT_PARTITIONS : partitions;
    replicationFactor = replicationFactor == null ? DEFAULT_REPLICATION_FACTOR : replicationFactor;
    configs = configs == null ? Map.of() : Map.copyOf(configs);
  }

  static TopicDefaults empty() {
    return new TopicDefaults(null, null, null);
  }
}
