package com.github.overz.shared.config;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * O GET no config-server, com retry. Nao sabe o que vem no corpo: devolve texto.
 *
 * Estatico e sem estado -- recebe para onde ir como argumento. E o que permite exercitar o
 * retry contra um servidor de teste sem precisar injetar nada no ConfigServerClient, que
 * continua se montando sozinho a partir do ambiente.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ConfigServerHttp {

  public static String get(final ConfigServerSettings settings) {
    final var request = HttpRequest.newBuilder()
      .uri(URI.create(settings.uri()))
      .timeout(settings.requestTimeout())
      .header("Accept", "application/json")
      .GET()
      .build();

    try (final var http = HttpClient.newBuilder().connectTimeout(settings.requestTimeout()).build()) {
      return send(http, request, settings);
    }
  }

  private static String send(
    final HttpClient http,
    final HttpRequest request,
    final ConfigServerSettings settings
  ) {
    Exception last = null;

    for (var attempt = 1; attempt <= settings.maxAttempts(); attempt++) {
      try {
        final var response = http.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() == 200) {
          return response.body();
        }

        last = new ConfigurationException(
          "config-server respondeu HTTP %d".formatted(response.statusCode()));
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ConfigurationException("interrompido ao consultar '%s'".formatted(settings.uri()), e);
      } catch (final Exception e) {
        last = e;
      }

      log.warn("falha ao consultar o config-server, tentativa={}/{}, url='{}', causa='{}'",
        attempt, settings.maxAttempts(), settings.uri(), last.getMessage());

      // O container pode ainda estar subindo: esperar entre as tentativas e o ponto do retry.
      if (attempt < settings.maxAttempts()) {
        sleep(settings.retryBackoff());
      }
    }

    throw new ConfigurationException(
      "config-server inalcancavel em '%s' apos %d tentativa(s)"
        .formatted(settings.uri(), settings.maxAttempts()), last);
  }

  private static void sleep(final Duration backoff) {
    try {
      Thread.sleep(backoff.toMillis());
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ConfigurationException("interrompido durante o backoff", e);
    }
  }
}
