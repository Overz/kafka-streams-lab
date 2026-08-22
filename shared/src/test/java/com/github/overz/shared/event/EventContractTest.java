package com.github.overz.shared.event;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.RecordComponent;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Contratos de evento do dominio")
class EventContractTest {

  private static List<String> componentesDe(final Class<?> record) {
    return Arrays.stream(record.getRecordComponents())
        .map(RecordComponent::getName)
        .toList();
  }

  @Nested
  @DisplayName("Dado o record OrderCreated")
  class OrderCreatedContrato {

    @Test
    @DisplayName("Quando inspecionado, Entao tem exatamente os componentes do documento")
    void componentes() {
      assertThat(componentesDe(OrderCreated.class))
          .containsExactly("orderId", "customerId", "items", "totalAmount", "currency", "createdAt");
    }

    @Test
    @DisplayName("Quando inspecionado, Entao items e uma lista de OrderItem")
    void tipoDosItens() {
      final var items = Arrays.stream(OrderCreated.class.getRecordComponents())
          .filter(c -> c.getName().equals("items"))
          .findFirst()
          .orElseThrow();

      assertThat(items.getType()).isEqualTo(List.class);
      assertThat(items.getGenericType().getTypeName()).contains(OrderItem.class.getName());
    }

    @Test
    @DisplayName("Quando inspecionado OrderItem, Entao tem productId, quantity e unitPrice")
    void componentesDoItem() {
      assertThat(componentesDe(OrderItem.class))
          .containsExactly("productId", "quantity", "unitPrice");
    }
  }

  @Nested
  @DisplayName("Dado o record FraudAnalysisCompleted")
  class FraudAnalysisCompletedContrato {

    @Test
    @DisplayName("Quando inspecionado, Entao tem exatamente os componentes do documento")
    void componentes() {
      assertThat(componentesDe(FraudAnalysisCompleted.class))
          .containsExactly("orderId", "customerId", "decision", "riskScore", "reasons", "analyzedAt");
    }

    @Test
    @DisplayName("Quando inspecionado o enum Decision, Entao tem APPROVED e REJECTED")
    void decisoes() {
      assertThat(Decision.values()).containsExactly(Decision.APPROVED, Decision.REJECTED);
    }

    @Test
    @DisplayName("Quando inspecionado o enum FraudReason, Entao contem as tres razoes do documento")
    void razoes() {
      assertThat(FraudReason.values()).contains(
          FraudReason.HIGH_ORDER_AMOUNT,
          FraudReason.TOO_MANY_ORDERS_IN_SHORT_PERIOD,
          FraudReason.AMOUNT_ABOVE_CUSTOMER_PROFILE);
    }
  }

  @Nested
  @DisplayName("Dado um evento ja construido")
  class Imutabilidade {

    @Test
    @DisplayName("Quando withTotalAmount e chamado, Entao devolve nova instancia e preserva a original")
    void with() {
      final var original = OrderCreated.builder()
          .orderId("ord-001")
          .customerId("cus-001")
          .items(List.of(new OrderItem("prod-001", 2, new BigDecimal("150.00"))))
          .totalAmount(new BigDecimal("300.00"))
          .currency("BRL")
          .createdAt(Instant.parse("2026-08-07T18:30:00Z"))
          .build();

      final var alterado = original.withTotalAmount(new BigDecimal("999.00"));

      assertThat(alterado).isNotSameAs(original);
      assertThat(alterado.totalAmount()).isEqualByComparingTo("999.00");
      assertThat(original.totalAmount()).isEqualByComparingTo("300.00");
      assertThat(alterado.orderId()).isEqualTo(original.orderId());
    }
  }
}
