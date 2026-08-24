package com.github.overz.shared.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.time.Duration;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ObjectMapperConfig {
  private static final ObjectMapper INSTANCE = JsonMapper.builder()
    .addModules(
      new JavaTimeModule(),
      new SimpleModule().addDeserializer(Duration.class, new SpringDurationDeserializer())
    )
    .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    .build()
    .setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL);

  private static final ObjectMapper JSON = INSTANCE.copy()
    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
    .enable(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN)
    ;

  private static final ObjectMapper YAML = JSON.copy()
    // As chaves do .yaml sao kebab-case; os componentes dos records sao camelCase.
    .setPropertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE);

  public static ObjectMapper json() {
    return JSON;
  }

  public static ObjectMapper yaml() {
    return YAML;
  }
}
