package com.github.overz.shared.config;

import com.sun.net.httpserver.HttpServer;
import lombok.Builder;
import lombok.With;
import lombok.extern.jackson.Jacksonized;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("Leitura da configuracao no config-server")
class ConfigServerClientTest {

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

  private HttpServer server;
  private final AtomicInteger chamadas = new AtomicInteger();

  private void responder(final int status, final String corpo) {
    server.createContext("/", exchange -> {
      chamadas.incrementAndGet();
      final var bytes = corpo.getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().add("Content-Type", "application/json");
      exchange.sendResponseHeaders(status, bytes.length);
      try (final var out = exchange.getResponseBody()) {
        out.write(bytes);
      }
    });
    server.start();
  }

  private String fixture() throws IOException {
    try (final InputStream in = getClass().getResourceAsStream("/config-server-orders-local.json")) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  private ConfigServerClient clienteApontandoParaOServidor() {
    return new ConfigServerClient(ConfigServerSettings.builder()
        .baseUrl("http://localhost:" + server.getAddress().getPort())
        .application("orders")
        .profile("local")
        .requestTimeout(Duration.ofSeconds(2))
        .maxAttempts(3)
        .retryBackoff(Duration.ofMillis(10))
        .build());
  }

  @BeforeEach
  void iniciarServidor() throws IOException {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
  }

  @AfterEach
  void pararServidor() {
    server.stop(0);
  }

  @Nested
  @DisplayName("Dada a resposta real do config-server")
  class RespostaReal {

    @Test
    @DisplayName("Quando ha conflito entre as fontes, Entao vence a mais especifica")
    void precedencia() throws IOException {
      responder(200, fixture());

      final var config = clienteApontandoParaOServidor().fetch(TestConfig.class);

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
      responder(200, fixture());

      final var config = clienteApontandoParaOServidor().fetch(TestConfig.class);

      assertThat(config.topics().orderCreated()).isEqualTo("orders.created");
      assertThat(config.topics().analysisCompleted()).isEqualTo("cluster-b.antifraud.analysis-completed");
    }

    @Test
    @DisplayName("Quando a chave e kebab-case, Entao mapeia para o componente camelCase")
    void kebabCase() throws IOException {
      responder(200, fixture());

      final var config = clienteApontandoParaOServidor().fetch(TestConfig.class);

      assertThat(config.kafka().clientId()).isEqualTo("orders");
    }

    @Test
    @DisplayName("Quando o valor e uma duracao no formato do Spring, Entao vira Duration")
    void duracao() throws IOException {
      responder(200, fixture());

      final var config = clienteApontandoParaOServidor().fetch(TestConfig.class);

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
      responder(200, fixture());

      final var config = clienteApontandoParaOServidor().fetch(TestConfig.class);

      assertThat(config.kafka().bootstrapServers())
          .containsExactly("localhost:19092", "localhost:19093", "localhost:19094");
    }

    @Test
    @DisplayName("Quando ha um unico endereco, Entao vira lista de um e nao string com virgulas")
    void umSo() {
      responder(200, """
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

      final var config = clienteApontandoParaOServidor().fetch(TestConfig.class);

      assertThat(config.kafka().bootstrapServers()).containsExactly("localhost:19092");
    }
  }

  @Nested
  @DisplayName("Dada uma configuracao que nao serve")
  class FalhaRapida {

    @Test
    @DisplayName("Quando falta uma chave obrigatoria, Entao falha nomeando a chave")
    void chaveAusente() {
      responder(200, """
          {"name":"orders","profiles":["local"],"propertySources":[
            {"name":"file:/config/orders.yaml","source":{
              "kafka.application-id":"orders-streams"
            }}]}
          """);

      final var cliente = clienteApontandoParaOServidor();

      assertThatThrownBy(() -> cliente.fetch(TestConfig.class))
          .isInstanceOf(ConfigurationException.class)
          .hasMessageContaining("kafka.bootstrap-servers");
    }

    @Test
    @DisplayName("Quando o tipo e incompativel, Entao falha nomeando a chave e o valor")
    void tipoIncompativel() {
      responder(200, """
          {"name":"orders","profiles":["local"],"propertySources":[
            {"name":"file:/config/orders.yaml","source":{
              "kafka.bootstrap-servers":"localhost:19092",
              "kafka.commit-interval":"nao-e-uma-duracao"
            }}]}
          """);

      final var cliente = clienteApontandoParaOServidor();

      assertThatThrownBy(() -> cliente.fetch(TestConfig.class))
          .isInstanceOf(ConfigurationException.class)
          .hasMessageContaining("commit-interval")
          .hasMessageContaining("nao-e-uma-duracao");
    }

    @Test
    @DisplayName("Quando o servidor devolve erro, Entao tenta de novo e desiste nomeando a URL")
    void servidorIndisponivel() {
      responder(503, "indisponivel");

      final var cliente = clienteApontandoParaOServidor();

      assertThatThrownBy(() -> cliente.fetch(TestConfig.class))
          .isInstanceOf(ConfigurationException.class)
          .hasMessageContaining("/orders/local");

      assertThat(chamadas.get()).isEqualTo(3);
    }
  }
}
