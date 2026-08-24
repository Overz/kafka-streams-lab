package com.github.overz.shared.streams;

import com.github.overz.shared.config.KafkaConfig;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.LogAndFailExceptionHandler;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Traduz o record de configuracao para as Properties que o Kafka Streams espera.
 * Um lugar so: a alternativa e cada Main montar as suas e divergirem no primeiro ajuste.
 */
@RequiredArgsConstructor
public final class StreamsProperties {

  private final KafkaConfig config;
  private final Map<String, Object> overrides;

  public StreamsProperties(final KafkaConfig config) {
    this(config, Map.of());
  }

  /**
   * Sobreposicao pontual, para o e2e trocar o application.id sem tocar no .yaml.
   */
  public StreamsProperties with(final Map<String, Object> extra) {
    final var merged = new LinkedHashMap<>(overrides);
    merged.putAll(extra);
    return new StreamsProperties(config, merged);
  }

  public Properties toProperties() {
    final var props = new Properties();

    props.put(StreamsConfig.APPLICATION_ID_CONFIG, config.applicationId());
    props.put(StreamsConfig.CLIENT_ID_CONFIG, config.clientId());
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(",", config.bootstrapServers()));
    props.put(StreamsConfig.STATE_DIR_CONFIG, config.stateDir());
    props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, config.numStreamThreads());
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, config.commitInterval().toMillis());
    props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, config.processingGuarantee());
    props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, config.replicationFactor());
    props.put(StreamsConfig.DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
      deserializationExceptionHandler());

    // Deixa o lab ja no comportamento que sera padrao no Kafka 5.0, em vez de herda-lo
    // num upgrade. Nao silencia o aviso por completo: o Kafka Streams monta instancias
    // internas de StreamsConfig que nao recebem esta propriedade e avisam de novo.
    props.put(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_GLOBAL_ENABLED_CONFIG, true);

    props.putAll(overrides);
    return props;
  }

  private Class<?> deserializationExceptionHandler() {
    return switch (config.deserializationExceptionHandler()) {
      case "log_and_continue" -> LogAndContinueExceptionHandler.class;
      case "log_and_fail" -> LogAndFailExceptionHandler.class;
      default -> throw new IllegalArgumentException(
        "kafka.deserialization-exception-handler invalido: '%s' (use log_and_continue ou log_and_fail)"
          .formatted(config.deserializationExceptionHandler()));
    };
  }
}
