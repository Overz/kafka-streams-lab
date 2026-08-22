package com.github.overz.shared.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Fabrica de serdes com cache por tipo. Instancia e nao estatico (regra 14): o Main monta
 * uma e a passa adiante, o que tambem permite um mapper diferente em teste.
 */
@RequiredArgsConstructor
public final class JsonSerdes {

  private final ObjectMapper mapper;
  private final Map<Class<?>, JsonSerde<?>> cache = new ConcurrentHashMap<>();

  public JsonSerdes() {
    this(JsonMapper.create());
  }

  @SuppressWarnings("unchecked")
  public <T> JsonSerde<T> of(final Class<T> type) {
    return (JsonSerde<T>) cache.computeIfAbsent(type, t -> new JsonSerde<>(mapper, type));
  }
}
