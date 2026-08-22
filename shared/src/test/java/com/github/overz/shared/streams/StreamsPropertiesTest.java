package com.github.overz.shared.streams;

import com.github.overz.shared.config.KafkaConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Traducao da configuracao para as Properties do Kafka Streams")
class StreamsPropertiesTest {

  private KafkaConfig config() {
    return KafkaConfig.builder()
        .bootstrapServers(List.of("localhost:19092", "localhost:19093"))
        .applicationId("orders-streams")
        .clientId("orders")
        .stateDir("/tmp/kafka-streams/orders")
        .numStreamThreads(3)
        .commitInterval(Duration.ofSeconds(1))
        .processingGuarantee("at_least_once")
        .deserializationExceptionHandler("log_and_continue")
        .replicationFactor(3)
        .build();
  }

  @Nested
  @DisplayName("Dada uma configuracao completa")
  class Traducao {

    @Test
    @DisplayName("Quando traduzida, Entao a lista de brokers vira uma string separada por virgula")
    void brokers() {
      final var props = new StreamsProperties(config()).toProperties();

      assertThat(props.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG))
          .isEqualTo("localhost:19092,localhost:19093");
    }

    @Test
    @DisplayName("Quando traduzida, Entao identidade e estado vao para as chaves do Streams")
    void identidade() {
      final var props = new StreamsProperties(config()).toProperties();

      assertThat(props.get(StreamsConfig.APPLICATION_ID_CONFIG)).isEqualTo("orders-streams");
      assertThat(props.get(StreamsConfig.CLIENT_ID_CONFIG)).isEqualTo("orders");
      assertThat(props.get(StreamsConfig.STATE_DIR_CONFIG)).isEqualTo("/tmp/kafka-streams/orders");
      assertThat(props.get(StreamsConfig.NUM_STREAM_THREADS_CONFIG)).isEqualTo(3);
    }

    @Test
    @DisplayName("Quando traduzida, Entao a duracao vira milissegundos")
    void duracaoEmMilissegundos() {
      final var props = new StreamsProperties(config()).toProperties();

      assertThat(props.get(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG)).isEqualTo(1_000L);
    }

    @Test
    @DisplayName("Quando traduzida, Entao a garantia de processamento e repassada")
    void garantia() {
      final var props = new StreamsProperties(config()).toProperties();

      assertThat(props.get(StreamsConfig.PROCESSING_GUARANTEE_CONFIG)).isEqualTo("at_least_once");
    }

    @Test
    @DisplayName("Quando traduzida, Entao aceita sobreposicoes pontuais sem tocar na config")
    void sobreposicoes() {
      final var props = new StreamsProperties(config())
          .with(Map.of(StreamsConfig.APPLICATION_ID_CONFIG, "orders-streams-e2e"))
          .toProperties();

      assertThat(props.get(StreamsConfig.APPLICATION_ID_CONFIG)).isEqualTo("orders-streams-e2e");
      assertThat(props.get(StreamsConfig.CLIENT_ID_CONFIG)).isEqualTo("orders");
    }

    @Test
    @DisplayName("Quando traduzida, Entao o handler global de excecao de processamento ja vem ligado")
    void handlerGlobalLigado() {
      final var props = new StreamsProperties(config()).toProperties();

      assertThat(props.get(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_GLOBAL_ENABLED_CONFIG))
          .isEqualTo(true);
    }

    @Test
    @DisplayName("Quando traduzida, Entao o handler de desserializacao e resolvido para a classe do Streams")
    void handlerDeDesserializacao() {
      final var props = new StreamsProperties(config()).toProperties();

      assertThat(props.get(StreamsConfig.DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG).toString())
          .contains("LogAndContinue");
    }
  }
}
