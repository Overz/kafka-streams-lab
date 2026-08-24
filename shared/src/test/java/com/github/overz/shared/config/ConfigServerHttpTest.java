package com.github.overz.shared.config;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("Consulta HTTP ao config-server")
class ConfigServerHttpTest {

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

  // ConfigServerSettings e um record: da para apontar para a porta sorteada sem que
  // ninguem precise injetar nada no ConfigServerClient.
  private ConfigServerSettings apontandoParaOServidor() {
    return ConfigServerSettings.builder()
      .baseUrl("http://localhost:" + server.getAddress().getPort())
      .prefix("/lab")
      .application("orders")
      .profile("local")
      .requestTimeout(Duration.ofSeconds(2))
      .maxAttempts(3)
      .retryBackoff(Duration.ofMillis(10))
      .build();
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
  @DisplayName("Dado um config-server que responde")
  class Responde {

    @Test
    @DisplayName("Quando devolve 200, Entao entrega o corpo sem tentar de novo")
    void corpo() {
      responder(200, "{\"name\":\"orders\"}");

      assertThat(ConfigServerHttp.get(apontandoParaOServidor())).isEqualTo("{\"name\":\"orders\"}");
      assertThat(chamadas.get()).isEqualTo(1);
    }
  }

  @Nested
  @DisplayName("Dado um config-server indisponivel")
  class Indisponivel {

    @Test
    @DisplayName("Quando o servidor devolve erro, Entao tenta de novo e desiste nomeando a URL")
    void servidorIndisponivel() {
      responder(503, "indisponivel");

      final var settings = apontandoParaOServidor();

      assertThatThrownBy(() -> ConfigServerHttp.get(settings))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("/orders/local");

      assertThat(chamadas.get()).isEqualTo(3);
    }
  }
}
