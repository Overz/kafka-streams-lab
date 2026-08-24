package com.github.overz.shared.config;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Converte as propriedades pontuadas do config-server na arvore que o Jackson espera.
 * <p>
 * O servidor devolve uma lista de propertySources, da mais especifica para a menos
 * (orders.yaml antes de application.yaml), cada uma com chaves achatadas:
 * <p>
 * {"kafka.bootstrap-servers": "a,b,c", "topics.order-created": "orders.created"}
 * <p>
 * Nao da para pedir a versao ja aninhada (/{app}-{profile}.json): aquele endpoint entrega
 * as fontes ja fundidas e tira do cliente a chance de aplicar a precedencia -- que e
 * justamente o que o config-server tem de util.
 */
final class ConfigTree {

  private ConfigTree() {
  }

  /**
   * A primeira fonte que define uma chave vence: a lista ja vem em ordem de precedencia.
   */
  static Map<String, Object> flatten(final List<Map<String, Object>> sources) {
    final var flat = new LinkedHashMap<String, Object>();
    for (final var source : sources) {
      source.forEach(flat::putIfAbsent);
    }
    return flat;
  }

  @SuppressWarnings("unchecked")
  static Map<String, Object> nest(final Map<String, Object> flat) {
    final var root = new LinkedHashMap<String, Object>();

    for (final var entry : flat.entrySet()) {
      // Split apenas na chave. O valor pode conter pontos -- o nome de topico
      // "cluster-b.antifraud.analysis-completed" e um so, nao tres niveis.
      final var segments = entry.getKey().split("\\.");
      var current = root;

      for (var i = 0; i < segments.length - 1; i++) {
        final var segment = segments[i];
        final var existing = current.get(segment);

        if (existing instanceof Map<?, ?> map) {
          current = (LinkedHashMap<String, Object>) map;
        } else {
          final var created = new LinkedHashMap<String, Object>();
          current.put(segment, created);
          current = created;
        }
      }

      current.put(segments[segments.length - 1], entry.getValue());
    }

    return root;
  }

  /**
   * Reconstroi a chave original a partir do caminho percorrido no record.
   */
  static String toPropertyKey(final List<String> camelCasePath) {
    final var segments = new ArrayList<String>(camelCasePath.size());
    for (final var segment : camelCasePath) {
      segments.add(toKebabCase(segment));
    }
    return String.join(".", segments);
  }

  static String toKebabCase(final String camelCase) {
    final var out = new StringBuilder(camelCase.length() + 4);
    for (final var c : camelCase.toCharArray()) {
      if (Character.isUpperCase(c)) {
        out.append('-').append(Character.toLowerCase(c));
      } else {
        out.append(c);
      }
    }
    return out.toString();
  }
}
