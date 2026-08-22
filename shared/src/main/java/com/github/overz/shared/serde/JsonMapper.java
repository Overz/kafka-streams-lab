package com.github.overz.shared.serde;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * O unico lugar onde o comportamento de JSON do lab e decidido. Um mapper por serde
 * abriria espaco para dois dominios divergirem na forma de escrever a mesma data.
 */
public final class JsonMapper {

  private JsonMapper() {
  }

  public static ObjectMapper create() {
    return new ObjectMapper()
        .registerModule(new JavaTimeModule())
        // Instante como "2026-08-07T18:30:00Z" e nao como epoch: o topico precisa ser
        // legivel por um humano no kafbat e consumivel pelo Flink SQL.
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        // Campo novo no produtor nao pode derrubar o consumidor -- os dois dominios
        // evoluem em ritmos diferentes.
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        // BigDecimal sem notacao cientifica: 2.0E+3 no topico e ilegivel.
        .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
        .enable(SerializationFeature.WRITE_BIGDECIMAL_AS_PLAIN)
        .setSerializationInclusion(JsonInclude.Include.NON_NULL);
  }
}
