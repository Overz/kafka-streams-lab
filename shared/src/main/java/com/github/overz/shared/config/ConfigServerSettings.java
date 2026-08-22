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
    String application,
    String profile,
    Duration requestTimeout,
    int maxAttempts,
    Duration retryBackoff
) {

  public static ConfigServerSettings fromEnvironment() {
    return ConfigServerSettings.builder()
        .application(required("APP_NAME"))
        .baseUrl(optional("CONFIG_SERVER_URL", "http://localhost:8888"))
        .profile(optional("CONFIG_PROFILE", "local"))
        .requestTimeout(Duration.parse(optional("CONFIG_REQUEST_TIMEOUT", "PT5S")))
        .maxAttempts(Integer.parseInt(optional("CONFIG_MAX_ATTEMPTS", "10")))
        .retryBackoff(Duration.parse(optional("CONFIG_RETRY_BACKOFF", "PT2S")))
        .build();
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

  public String path() {
    return "/%s/%s".formatted(application, profile);
  }

  public String uri() {
    return baseUrl + path();
  }
}
