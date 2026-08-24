package com.github.overz.shared.config;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * O corpo devolvido pelo config-server virando record tipado e validado.
 *
 * Nenhuma rede aqui: e funcao de texto para objeto. Precedencia entre fontes, achatamento
 * de chave pontuada, kebab-case e a mensagem de erro sao o que de fato pode quebrar, e
 * tudo isso se exercita com uma String.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ConfigServerResponse {

  private static final ObjectMapper MAPPER = ObjectMapperConfig.yaml();

  public static <T> T parse(
    final ConfigServerSettings settings,
    final String body,
    final Class<T> type
  ) {
    final var sources = readPropertySources(settings, body);

    log.info("configuracao recebida, aplicacao='{}', fontes={}, chaves={}",
      settings.application(), sources.size(), sources.stream().mapToInt(Map::size).sum());

    final var config = convert(settings, ConfigTree.nest(ConfigTree.flatten(sources)), type);

    RequiredPropertiesValidator.validate(config, settings.application());

    return config;
  }

  @SuppressWarnings("unchecked")
  private static List<Map<String, Object>> readPropertySources(
    final ConfigServerSettings settings,
    final String body
  ) {
    try {
      final var root = MAPPER.readTree(body);
      final var sources = root.path("propertySources");

      if (!sources.isArray() || sources.isEmpty()) {
        throw new ConfigurationException(
          "config-server nao devolveu nenhuma fonte para '%s' -- confira se o arquivo '%s.yaml' existe em /config"
            .formatted(settings.path(), settings.application()));
      }

      final var result = new ArrayList<Map<String, Object>>(sources.size());
      for (final var source : sources) {
        result.add(MAPPER.convertValue(source.path("source"), Map.class));
      }
      return result;
    } catch (final Exception e) {
      throw new ConfigurationException(
        "resposta do config-server em '%s' nao pode ser lida".formatted(settings.uri()), e);
    }
  }

  private static <T> T convert(
    final ConfigServerSettings settings,
    final Map<String, Object> tree,
    final Class<T> type
  ) {
    try {
      return MAPPER.convertValue(tree, type);
    } catch (final Exception e) {
      throw new ConfigurationException(
        "propriedade invalida para a aplicacao '%s': %s".formatted(settings.application(), describe(e)), e);
    }
  }

  /**
   * Junta o caminho que o Jackson percorreu com a mensagem da causa raiz.
   */
  private static String describe(final Exception e) {
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
