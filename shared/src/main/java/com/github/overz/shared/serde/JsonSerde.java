package com.github.overz.shared.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

@RequiredArgsConstructor
public final class JsonSerde<T> implements Serde<T> {

  private final ObjectMapper mapper;
  private final Class<T> type;

  @Override
  public Serializer<T> serializer() {
    return (topic, data) -> {
      if (data == null) {
        return null;
      }
      try {
        return mapper.writeValueAsBytes(data);
      } catch (final Exception e) {
        throw new SerializationException(
            "falha ao serializar %s para o topico '%s'".formatted(type.getSimpleName(), topic), e);
      }
    };
  }

  @Override
  public Deserializer<T> deserializer() {
    return (topic, data) -> {
      if (data == null) {
        return null;
      }
      try {
        return mapper.readValue(data, type);
      } catch (final Exception e) {
        throw new SerializationException(
            "falha ao desserializar %s do topico '%s'".formatted(type.getSimpleName(), topic), e);
      }
    };
  }
}
