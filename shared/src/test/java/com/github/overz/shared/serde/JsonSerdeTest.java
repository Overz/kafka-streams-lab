package com.github.overz.shared.serde;

import com.github.overz.shared.event.Decision;
import com.github.overz.shared.event.FraudAnalysisCompleted;
import com.github.overz.shared.event.FraudReason;
import com.github.overz.shared.event.OrderCreated;
import com.github.overz.shared.event.OrderItem;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("Serializacao JSON dos eventos")
class JsonSerdeTest {

  private static final String TOPICO = "orders.created";

  private final JsonSerdes serdes = new JsonSerdes();

  private OrderCreated pedido() {
    return OrderCreated.builder()
        .orderId("ord-001")
        .customerId("cus-001")
        .items(List.of(new OrderItem("prod-001", 2, new BigDecimal("150.00"))))
        .totalAmount(new BigDecimal("300.00"))
        .currency("BRL")
        .createdAt(Instant.parse("2026-08-07T18:30:00Z"))
        .build();
    }

  @Nested
  @DisplayName("Dado um evento valido")
  class RoundTrip {

    @Test
    @DisplayName("Quando serializado e desserializado, Entao o record e identico ao original")
    void preservaOEvento() {
      final var serde = serdes.of(OrderCreated.class);
      final var original = pedido();

      final var bytes = serde.serializer().serialize(TOPICO, original);
      final var reconstruido = serde.deserializer().deserialize(TOPICO, bytes);

      assertThat(reconstruido).isEqualTo(original);
    }

    @Test
    @DisplayName("Quando serializado, Entao o instante sai em ISO-8601 e nao em epoch")
    void instanteIso8601() {
      final var serde = serdes.of(OrderCreated.class);

      final var json = new String(
          serde.serializer().serialize(TOPICO, pedido()), StandardCharsets.UTF_8);

      assertThat(json).contains("\"createdAt\":\"2026-08-07T18:30:00Z\"");
      assertThat(json).doesNotContain("1786905000");
    }

    @Test
    @DisplayName("Quando o evento tem enums, Entao saem como nome e voltam como enum")
    void enums() {
      final var serde = serdes.of(FraudAnalysisCompleted.class);
      final var analise = FraudAnalysisCompleted.builder()
          .orderId("ord-002")
          .customerId("cus-001")
          .decision(Decision.REJECTED)
          .riskScore(90)
          .reasons(List.of(FraudReason.HIGH_ORDER_AMOUNT, FraudReason.AMOUNT_ABOVE_CUSTOMER_PROFILE))
          .analyzedAt(Instant.parse("2026-08-07T18:30:02Z"))
          .build();

      final var json = new String(
          serde.serializer().serialize(TOPICO, analise), StandardCharsets.UTF_8);
      assertThat(json).contains("\"decision\":\"REJECTED\"").contains("\"HIGH_ORDER_AMOUNT\"");

      assertThat(serde.deserializer().deserialize(TOPICO, json.getBytes(StandardCharsets.UTF_8)))
          .isEqualTo(analise);
    }
  }

  @Nested
  @DisplayName("Dado um payload que o consumidor nao reconhece por inteiro")
  class Tolerancia {

    @Test
    @DisplayName("Quando o JSON tem campo desconhecido, Entao o campo e ignorado")
    void campoDesconhecido() {
      final var json = """
          {"orderId":"ord-001","customerId":"cus-001","items":[],
           "totalAmount":300.00,"currency":"BRL","createdAt":"2026-08-07T18:30:00Z",
           "campoQueOProdutorAdicionouDepois":"valor"}
          """;

      final var resultado = serdes.of(OrderCreated.class)
          .deserializer()
          .deserialize(TOPICO, json.getBytes(StandardCharsets.UTF_8));

      assertThat(resultado.orderId()).isEqualTo("ord-001");
      assertThat(resultado.totalAmount()).isEqualByComparingTo("300.00");
    }
  }

  @Nested
  @DisplayName("Dado um payload problematico")
  class Falhas {

    @Test
    @DisplayName("Quando os bytes nao sao JSON valido, Entao lanca SerializationException nomeando o topico")
    void payloadCorrompido() {
      final var deserializer = serdes.of(OrderCreated.class).deserializer();
      final var lixo = "isto nao e json".getBytes(StandardCharsets.UTF_8);

      assertThatThrownBy(() -> deserializer.deserialize(TOPICO, lixo))
          .isInstanceOf(SerializationException.class)
          .hasMessageContaining(TOPICO);
    }

    @Test
    @DisplayName("Quando o valor e nulo, Entao devolve nulo sem lancar")
    void tombstone() {
      final var serde = serdes.of(OrderCreated.class);

      assertThat(serde.serializer().serialize(TOPICO, null)).isNull();
      assertThat(serde.deserializer().deserialize(TOPICO, null)).isNull();
    }
  }
}
