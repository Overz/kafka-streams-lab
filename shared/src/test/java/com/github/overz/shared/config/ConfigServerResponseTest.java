package com.github.overz.shared.config;

import lombok.Builder;
import lombok.With;
import lombok.extern.jackson.Jacksonized;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("Leitura da resposta do config-server")
class ConfigServerResponseTest {

  @With
  @Builder
  @Jacksonized
  record OrdersTopics(String orderCreated, String analysisCompleted, String deadLetter) {
  }

  @With
  @Builder
  @Jacksonized
  record TestConfig(KafkaConfig kafka, TelemetryConfig telemetry, OrdersTopics topics) {
  }

  // Nao vem do ambiente: o parsing nao usa nada daqui alem do nome da aplicacao e da URL
  // que aparecem nas mensagens de erro.
  private static final ConfigServerSettings SETTINGS = ConfigServerSettings.builder()
    .baseUrl("http://config-server:8888")
    .prefix("/lab")
    .application("orders")
    .profile("local")
    .requestTimeout(Duration.ofSeconds(2))
    .maxAttempts(3)
    .retryBackoff(Duration.ofMillis(10))
    .build();

  private TestConfig parse(final String body) {
    return ConfigServerResponse.parse(SETTINGS, body, TestConfig.class);
  }

  private String fixture() throws IOException {
    try (final InputStream in = getClass().getResourceAsStream("/config-server-orders-local.json")) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  @Nested
  @DisplayName("Dada a resposta real do config-server")
  class RespostaReal {

    @Test
    @DisplayName("Quando ha conflito entre as fontes, Entao vence a mais especifica")
    void precedencia() throws IOException {
      final var config = parse(fixture());

      // orders.yaml define state-dir; application.yaml nao. E orders.yaml vem primeiro na
      // lista, entao qualquer chave repetida nele tem de vencer a de application.yaml.
      assertThat(config.kafka().stateDir()).isEqualTo("/tmp/kafka-streams/orders");
      assertThat(config.kafka().applicationId()).isEqualTo("orders-streams");
      // Veio de application.yaml, a fonte de menor precedencia.
      assertThat(config.kafka().numStreamThreads()).isEqualTo(3);
    }

    @Test
    @DisplayName("Quando a chave e pontuada, Entao vira record aninhado")
    void aninhamento() throws IOException {
      final var config = parse(fixture());

      assertThat(config.topics().orderCreated()).isEqualTo("orders.created");
      assertThat(config.topics().analysisCompleted()).isEqualTo("cluster-b.antifraud.analysis-completed");
    }

    @Test
    @DisplayName("Quando a chave e kebab-case, Entao mapeia para o componente camelCase")
    void kebabCase() throws IOException {
      final var config = parse(fixture());

      assertThat(config.kafka().clientId()).isEqualTo("orders");
    }

    @Test
    @DisplayName("Quando o valor e uma duracao no formato do Spring, Entao vira Duration")
    void duracao() throws IOException {
      final var config = parse(fixture());

      assertThat(config.kafka().commitInterval()).isEqualTo(Duration.ofSeconds(1));
      assertThat(config.telemetry().timeout()).isEqualTo(Duration.ofSeconds(10));
    }
  }

  @Nested
  @DisplayName("Dada uma propriedade de lista")
  class Listas {

    @Test
    @DisplayName("Quando ha varios enderecos separados por virgula, Entao vira lista de varios")
    void varios() throws IOException {
      final var config = parse(fixture());

      assertThat(config.kafka().bootstrapServers())
        .containsExactly("localhost:19092", "localhost:19093", "localhost:19094");
    }

    @Test
    @DisplayName("Quando ha um unico endereco, Entao vira lista de um e nao string com virgulas")
    void umSo() {
      final var config = parse("""
        {"name":"orders","profiles":["local"],"propertySources":[
          {"name":"file:/config/orders.yaml","source":{
            "kafka.bootstrap-servers":"localhost:19092",
            "kafka.application-id":"orders-streams",
            "kafka.client-id":"orders",
            "kafka.state-dir":"/tmp/x",
            "kafka.num-stream-threads":1,
            "kafka.commit-interval":"1s",
            "kafka.processing-guarantee":"at_least_once",
            "kafka.deserialization-exception-handler":"log_and_continue",
            "kafka.replication-factor":1,
            "telemetry.endpoint":"http://localhost:4317",
            "telemetry.timeout":"10s",
            "topics.order-created":"orders.created",
            "topics.analysis-completed":"antifraud.analysis-completed",
            "topics.dead-letter":"orders.dlq"
          }}]}
        """);

      assertThat(config.kafka().bootstrapServers()).containsExactly("localhost:19092");
    }
  }

  @Nested
  @DisplayName("Dada uma configuracao que nao serve")
  class FalhaRapida {

    @Test
    @DisplayName("Quando falta uma chave obrigatoria, Entao falha nomeando a chave")
    void chaveAusente() {
      final var body = """
        {"name":"orders","profiles":["local"],"propertySources":[
          {"name":"file:/config/orders.yaml","source":{
            "kafka.application-id":"orders-streams"
          }}]}
        """;

      assertThatThrownBy(() -> parse(body))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("kafka.bootstrap-servers");
    }

    @Test
    @DisplayName("Quando o tipo e incompativel, Entao falha nomeando a chave e o valor")
    void tipoIncompativel() {
      final var body = """
        {"name":"orders","profiles":["local"],"propertySources":[
          {"name":"file:/config/orders.yaml","source":{
            "kafka.bootstrap-servers":"localhost:19092",
            "kafka.commit-interval":"nao-e-uma-duracao"
          }}]}
        """;

      assertThatThrownBy(() -> parse(body))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("commit-interval")
        .hasMessageContaining("nao-e-uma-duracao");
    }

    @Test
    @DisplayName("Quando nao ha nenhuma fonte, Entao falha dizendo qual arquivo procurar")
    void semFonte() {
      final var body = """
        {"name":"orders","profiles":["local"],"propertySources":[]}
        """;

      // A dica de qual arquivo criar ficou na causa: readPropertySources nao reergue mais
      // a ConfigurationException, entao ela e reembrulhada na mensagem generica.
      assertThatThrownBy(() -> parse(body))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("/lab/orders/local")
        .cause()
        .hasMessageContaining("orders.yaml");
    }
  }
}
