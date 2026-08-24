package com.github.overz.shared.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Endereco do config-server")
class ConfigServerSettingsTest {

  private static ConfigServerSettings.ConfigServerSettingsBuilder base() {
    return ConfigServerSettings.builder()
      .baseUrl("http://config-server:8888")
      .prefix("/lab")
      .application("orders")
      .requestTimeout(Duration.ofSeconds(2))
      .maxAttempts(3)
      .retryBackoff(Duration.ofMillis(10));
  }

  @Nested
  @DisplayName("Dado o caso normal, sem profile")
  class SemProfile {

    @Test
    @DisplayName("Quando o profile e ausente, Entao a consulta e namespace + aplicacao")
    void ausente() {
      assertThat(base().build().path()).isEqualTo("/lab/orders");
      assertThat(base().build().uri()).isEqualTo("http://config-server:8888/lab/orders");
    }

    @Test
    @DisplayName("Quando o profile e vazio ou em branco, Entao vale o mesmo que ausente")
    void vazio() {
      assertThat(base().profile("").build().path()).isEqualTo("/lab/orders");
      assertThat(base().profile("   ").build().path()).isEqualTo("/lab/orders");
    }
  }

  @Nested
  @DisplayName("Dado um profile explicito")
  class ComProfile {

    @Test
    @DisplayName("Quando ha profile, Entao ele vira o terceiro segmento")
    void terceiroSegmento() {
      assertThat(base().profile("prod").build().path()).isEqualTo("/lab/orders/prod");
    }
  }

  @Nested
  @DisplayName("Dado um prefixo fora do formato")
  class Prefixo {

    @Test
    @DisplayName("Quando o prefixo nao tem barra inicial, Entao ela e acrescentada")
    void semBarraInicial() {
      assertThat(base().prefix("lab").build().path()).isEqualTo("/lab/orders");
    }

    @Test
    @DisplayName("Quando o prefixo termina em barra, Entao ela nao duplica")
    void barraFinal() {
      assertThat(base().prefix("/lab/").build().path()).isEqualTo("/lab/orders");
    }

    // Um config-server sem spring.cloud.config.server.prefix continua alcancavel: a URL
    // volta a ser a do Spring Cloud puro, sem namespace.
    @Test
    @DisplayName("Quando nao ha prefixo, Entao a consulta vai para a raiz do servidor")
    void semPrefixo() {
      assertThat(base().prefix(null).build().path()).isEqualTo("/orders");
      assertThat(base().prefix("").build().path()).isEqualTo("/orders");
      assertThat(base().prefix("/").build().path()).isEqualTo("/orders");
    }
  }
}
