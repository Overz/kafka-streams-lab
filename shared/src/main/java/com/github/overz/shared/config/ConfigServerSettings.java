package com.github.overz.shared.config;

import lombok.Builder;
import lombok.With;

import java.time.Duration;

// Como alcancar o config-server. E a unica configuracao que nao pode vir dele, entao vem
// do ambiente.
//
// APP_NAME e obrigatoria e sem valor padrao: e ela que decide qual arquivo o config-server
// serve. Um padrao aqui faria a aplicacao subir com a configuracao de outro dominio quando
// a variavel faltasse, que e pior do que nao subir.
@With
@Builder
public record ConfigServerSettings(
    String baseUrl,
    String prefix,
    String application,
    String profile,
    Duration requestTimeout,
    int maxAttempts,
    Duration retryBackoff
) {

  private static final class Holder {
    private static final ConfigServerSettings INSTANCE = ConfigServerSettings.builder()
      .application(required("APP_NAME"))
      .baseUrl(optional("CONFIG_SERVER_URL", "http://localhost:8888"))
      // Namespace do lab dentro do config-server (spring.cloud.config.server.prefix).
      .prefix(optional("CONFIG_SERVER_PREFIX", "/lab"))
      // Vazio de proposito: a consulta normal e /lab/{app}, e quem escolhe o profile e o
      // servidor (lab.config.default-profile). Definir CONFIG_PROFILE volta a forma
      // longa, /lab/{app}/{profile}, para apontar uma instancia a outro profile sem
      // mexer no config-server.
      .profile(optional("CONFIG_PROFILE", ""))
      .requestTimeout(Duration.parse(optional("CONFIG_REQUEST_TIMEOUT", "PT5S")))
      .maxAttempts(Integer.parseInt(optional("CONFIG_MAX_ATTEMPTS", "10")))
      .retryBackoff(Duration.parse(optional("CONFIG_RETRY_BACKOFF", "PT2S")))
      .build();
  }

  public static ConfigServerSettings shared() {
    return Holder.INSTANCE;
  }

  private static String required(final String key) {
    final var value = System.getenv(key);

    if (value == null || value.isBlank()) {
      throw new ConfigurationException(
          "variavel de ambiente obrigatoria '%s' nao definida".formatted(key));
    }

    return value;
  }

  private static String optional(final String key, final String fallback) {
    final var value = System.getenv(key);
    return value == null || value.isBlank() ? fallback : value;
  }

  /**
   * {@code /lab/orders} no caso normal, {@code /lab/orders/prod} quando ha profile.
   *
   * Sem profile quem resolve e o config-server, pelo atalho namespace + aplicacao que ele
   * expoe; o Spring Cloud sozinho exigiria os tres segmentos.
   */
  public String path() {
    final var base = "%s/%s".formatted(normalizedPrefix(), application);
    return profile == null || profile.isBlank() ? base : base + "/" + profile;
  }

  // Tolera prefixo ausente, vazio ou sem a barra inicial: o servidor pode estar sem
  // spring.cloud.config.server.prefix, e ai a URL volta a ser /{app}.
  private String normalizedPrefix() {
    if (prefix == null || prefix.isBlank() || "/".equals(prefix)) {
      return "";
    }

    final var trimmed = prefix.endsWith("/") ? prefix.substring(0, prefix.length() - 1) : prefix;
    return trimmed.startsWith("/") ? trimmed : "/" + trimmed;
  }

  public String uri() {
    return baseUrl + path();
  }
}
