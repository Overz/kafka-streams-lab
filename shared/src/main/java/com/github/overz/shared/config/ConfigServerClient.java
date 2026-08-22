package com.github.overz.shared.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Le a configuracao da aplicacao no config-server durante o boot e a entrega como record
 * tipado. Usa o HttpClient do JDK: um GET nao justifica arrastar o Spring inteiro para
 * dentro de um lab cujo assunto e Kafka Streams puro.
 */
@RequiredArgsConstructor
@Slf4j
public final class ConfigServerClient {

  private final ConfigServerSettings settings;
  private final HttpClient http;
  private final ObjectMapper mapper;

  public ConfigServerClient(final ConfigServerSettings settings) {
    this(settings, defaultHttpClient(settings), defaultMapper());
  }

  // Um processo fala com um config-server so, e a configuracao e lida uma vez no boot.
  // Guardar a instancia evita que cada ponto que precise do nome da aplicacao ou de uma
  // secao da config monte um cliente novo -- e nao e static: continua sendo um objeto,
  // criado sob demanda a partir do ambiente.
  private static final class Holder {
    private static final ConfigServerClient INSTANCE =
        new ConfigServerClient(ConfigServerSettings.fromEnvironment());
  }

  public static ConfigServerClient shared() {
    return Holder.INSTANCE;
  }

  public String application() {
    return settings.application();
  }

  private static HttpClient defaultHttpClient(final ConfigServerSettings settings) {
    return HttpClient.newBuilder()
        .connectTimeout(settings.requestTimeout())
        .build();
  }

  private static ObjectMapper defaultMapper() {
    final var module = new SimpleModule()
        .addDeserializer(Duration.class, new SpringDurationDeserializer());

    return new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .registerModule(module)
        // As chaves do .yaml sao kebab-case; os componentes dos records sao camelCase.
        .setPropertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE)
        // Chave a mais no .yaml e ruido, nao motivo para derrubar a app. Chave a MENOS e
        // que e fatal, e disso cuida o RequiredPropertiesValidator.
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
  }

  public <T> T fetch(final Class<T> type) {
    log.info("carregando configuracao, aplicacao='{}', perfil='{}', url='{}'",
        settings.application(), settings.profile(), settings.uri());

    final var body = get();
    final var sources = readPropertySources(body);

    log.info("configuracao recebida, aplicacao='{}', fontes={}, chaves={}",
        settings.application(), sources.size(), sources.stream().mapToInt(Map::size).sum());

    final var tree = ConfigTree.nest(ConfigTree.flatten(sources));
    final var config = convert(tree, type);

    RequiredPropertiesValidator.validate(config, settings.application());

    log.info("configuracao carregada e validada, aplicacao='{}', tipo='{}'",
        settings.application(), type.getSimpleName());

    return config;
  }

  private String get() {
    final var request = HttpRequest.newBuilder()
        .uri(URI.create(settings.uri()))
        .timeout(settings.requestTimeout())
        .header("Accept", "application/json")
        .GET()
        .build();

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

  private void sleep(final Duration backoff) {
    try {
      Thread.sleep(backoff.toMillis());
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ConfigurationException("interrompido durante o backoff", e);
    }
  }

  @SuppressWarnings("unchecked")
  private List<Map<String, Object>> readPropertySources(final String body) {
    try {
      final var root = mapper.readTree(body);
      final var sources = root.path("propertySources");

      if (!sources.isArray() || sources.isEmpty()) {
        throw new ConfigurationException(
            "config-server nao devolveu nenhuma fonte para '%s' -- confira se o arquivo '%s.yaml' existe em /config"
                .formatted(settings.path(), settings.application()));
      }

      final var result = new ArrayList<Map<String, Object>>(sources.size());
      for (final var source : sources) {
        result.add(mapper.convertValue(source.path("source"), Map.class));
      }
      return result;
    } catch (final ConfigurationException e) {
      throw e;
    } catch (final Exception e) {
      throw new ConfigurationException(
          "resposta do config-server em '%s' nao pode ser lida".formatted(settings.uri()), e);
    }
  }

  private <T> T convert(final Map<String, Object> tree, final Class<T> type) {
    try {
      return mapper.convertValue(tree, type);
    } catch (final IllegalArgumentException e) {
      throw new ConfigurationException(
          "propriedade invalida para a aplicacao '%s': %s".formatted(settings.application(), describe(e)), e);
    }
  }

  /** Junta o caminho que o Jackson percorreu com a mensagem da causa raiz. */
  private String describe(final IllegalArgumentException e) {
    if (!(e.getCause() instanceof JsonMappingException mapping)) {
      return e.getMessage();
    }

    final var path = mapping.getPath().stream()
        .map(JsonMappingException.Reference::getFieldName)
        .filter(Objects::nonNull)
        .map(ConfigTree::toKebabCase)
        .toList();

    var cause = (Throwable) mapping;
    while (cause.getCause() != null) {
      cause = cause.getCause();
    }

    return "%s -> %s".formatted(String.join(".", path), cause.getMessage());
  }
}
