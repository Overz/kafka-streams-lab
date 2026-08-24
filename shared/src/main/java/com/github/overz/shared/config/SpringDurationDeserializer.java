package com.github.overz.shared.config;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import java.time.Duration;
import java.util.regex.Pattern;

/**
 * Aceita as duracoes no formato que se escreve num .yaml de Spring ("1s", "5m", "24h"),
 * alem do ISO-8601 que o Jackson ja entende. Sem isto, "1s" no config-server chegaria
 * como texto invalido e o boot falharia por um motivo que parece um erro de digitacao.
 */
public final class SpringDurationDeserializer extends JsonDeserializer<Duration> {

  private static final Pattern SIMPLE = Pattern.compile("^([+-]?\\d+)(ns|us|ms|s|m|h|d)$");

  @Override
  public Duration deserialize(final JsonParser parser, final DeserializationContext context)
    throws IOException {
    final var raw = parser.getText().trim();

    if (raw.startsWith("P") || raw.startsWith("-P")) {
      return Duration.parse(raw);
    }

    final var matcher = SIMPLE.matcher(raw);
    if (!matcher.matches()) {
      throw new IllegalArgumentException(
        "duracao invalida: '%s' (use 1s, 5m, 24h ou ISO-8601 PT1S)".formatted(raw));
    }

    final var amount = Long.parseLong(matcher.group(1));
    return switch (matcher.group(2)) {
      case "ns" -> Duration.ofNanos(amount);
      case "us" -> Duration.ofNanos(amount * 1_000L);
      case "ms" -> Duration.ofMillis(amount);
      case "s" -> Duration.ofSeconds(amount);
      case "m" -> Duration.ofMinutes(amount);
      case "h" -> Duration.ofHours(amount);
      case "d" -> Duration.ofDays(amount);
      default -> throw new IllegalStateException("unidade nao tratada: " + matcher.group(2));
    };
  }
}
