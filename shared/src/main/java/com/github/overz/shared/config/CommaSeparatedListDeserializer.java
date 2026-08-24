package com.github.overz.shared.config;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Uma propriedade de lista chega do config-server como "a,b,c" numa unica string.
 * Devolver List.of(valorInteiro) transformaria os tres brokers do cluster num unico
 * host com virgulas no nome -- o mesmo defeito que a API em Go ja documenta em
 * internal/config/config.go.
 */
public final class CommaSeparatedListDeserializer extends JsonDeserializer<List<String>> {

  @Override
  public List<String> deserialize(
    final JsonParser parser,
    final DeserializationContext context
  ) throws IOException {

    if (parser.currentToken() == JsonToken.START_ARRAY) {
      final var values = new ArrayList<String>();
      while (parser.nextToken() != JsonToken.END_ARRAY) {
        values.add(parser.getText().trim());
      }
      return List.copyOf(values);
    }

    final var raw = parser.getText().trim();
    if (raw.isEmpty()) {
      return List.of();
    }

    return Arrays.stream(raw.split(","))
      .map(String::trim)
      .filter(s -> !s.isEmpty())
      .toList();
  }
}
