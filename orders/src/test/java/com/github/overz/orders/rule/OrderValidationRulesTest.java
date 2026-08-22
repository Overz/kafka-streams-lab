package com.github.overz.orders.rule;

import com.github.overz.shared.event.Money;
import com.github.overz.shared.event.OrderCreated;
import com.github.overz.shared.event.OrderItem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Regras de validacao do pedido")
class OrderValidationRulesTest {

  private final Money money = Money.brl();
  private final OrderValidationRules rules = OrderValidationRules.standard(money);

  private OrderCreated.OrderCreatedBuilder pedidoValido() {
    return OrderCreated.builder()
        .orderId("ord-001")
        .customerId("cus-001")
        .items(List.of(new OrderItem("prod-001", 2, new BigDecimal("150.00"))))
        .totalAmount(new BigDecimal("300.00"))
        .currency("BRL")
        .createdAt(Instant.parse("2026-08-07T18:30:00Z"));
  }

  private List<String> motivosDe(final OrderCreated pedido) {
    return rules.validate(pedido).stream().map(OrderValidationRule.Violation::reason).toList();
  }

  @Nested
  @DisplayName("Dado um pedido completo")
  class PedidoValido {

    @Test
    @DisplayName("Quando validado, Entao nenhuma regra e violada")
    void semViolacoes() {
      assertThat(rules.validate(pedidoValido().build())).isEmpty();
    }

    @Test
    @DisplayName("Quando o total tem escala diferente da soma, Entao continua valido")
    void escalaDiferente() {
      final var pedido = pedidoValido().totalAmount(new BigDecimal("300.0")).build();

      assertThat(rules.validate(pedido)).isEmpty();
    }

    @Test
    @DisplayName("Quando ha varios itens, Entao o total e a soma de todos")
    void variosItens() {
      final var pedido = pedidoValido()
          .items(List.of(
              new OrderItem("prod-001", 2, new BigDecimal("150.00")),
              new OrderItem("prod-002", 1, new BigDecimal("100.00"))))
          .totalAmount(new BigDecimal("400.00"))
          .build();

      assertThat(rules.validate(pedido)).isEmpty();
    }
  }

  @Nested
  @DisplayName("Dado um pedido sem campo obrigatorio")
  class CamposObrigatorios {

    @Test
    @DisplayName("Quando falta orderId, Entao o motivo e ORDER_ID_REQUIRED")
    void semOrderId() {
      assertThat(motivosDe(pedidoValido().orderId(null).build())).contains("ORDER_ID_REQUIRED");
    }

    @Test
    @DisplayName("Quando orderId e apenas espacos, Entao tambem e ORDER_ID_REQUIRED")
    void orderIdEmBranco() {
      assertThat(motivosDe(pedidoValido().orderId("   ").build())).contains("ORDER_ID_REQUIRED");
    }

    @Test
    @DisplayName("Quando falta customerId, Entao o motivo e CUSTOMER_ID_REQUIRED")
    void semCustomerId() {
      assertThat(motivosDe(pedidoValido().customerId(null).build())).contains("CUSTOMER_ID_REQUIRED");
    }

    @Test
    @DisplayName("Quando falta currency, Entao o motivo e CURRENCY_REQUIRED")
    void semCurrency() {
      assertThat(motivosDe(pedidoValido().currency(null).build())).contains("CURRENCY_REQUIRED");
    }

    @Test
    @DisplayName("Quando falta createdAt, Entao o motivo e CREATED_AT_REQUIRED")
    void semCreatedAt() {
      assertThat(motivosDe(pedidoValido().createdAt(null).build())).contains("CREATED_AT_REQUIRED");
    }

    @Test
    @DisplayName("Quando faltam orderId e currency, Entao os dois motivos aparecem")
    void doisMotivos() {
      final var motivos = motivosDe(pedidoValido().orderId(null).currency(null).build());

      assertThat(motivos).contains("ORDER_ID_REQUIRED", "CURRENCY_REQUIRED");
    }
  }

  @Nested
  @DisplayName("Dado um pedido com problema nos itens")
  class Itens {

    @Test
    @DisplayName("Quando a lista de itens e vazia, Entao o motivo e ITEMS_REQUIRED")
    void semItens() {
      assertThat(motivosDe(pedidoValido().items(List.of()).build())).contains("ITEMS_REQUIRED");
    }

    @Test
    @DisplayName("Quando a lista de itens e nula, Entao o motivo e ITEMS_REQUIRED")
    void itensNulos() {
      assertThat(motivosDe(pedidoValido().items(null).build())).contains("ITEMS_REQUIRED");
    }

    @Test
    @DisplayName("Quando um item nao tem productId, Entao o motivo e ITEM_PRODUCT_ID_REQUIRED")
    void semProductId() {
      final var pedido = pedidoValido()
          .items(List.of(new OrderItem(null, 2, new BigDecimal("150.00")))).build();

      assertThat(motivosDe(pedido)).contains("ITEM_PRODUCT_ID_REQUIRED");
    }

    @Test
    @DisplayName("Quando um item tem quantidade zero, Entao o motivo e ITEM_QUANTITY_MUST_BE_POSITIVE")
    void quantidadeZero() {
      final var pedido = pedidoValido()
          .items(List.of(new OrderItem("prod-001", 0, new BigDecimal("150.00"))))
          .totalAmount(new BigDecimal("0.00"))
          .build();

      assertThat(motivosDe(pedido)).contains("ITEM_QUANTITY_MUST_BE_POSITIVE");
    }

    @Test
    @DisplayName("Quando um item tem preco zero, Entao o motivo e ITEM_UNIT_PRICE_MUST_BE_POSITIVE")
    void precoZero() {
      final var pedido = pedidoValido()
          .items(List.of(new OrderItem("prod-001", 2, BigDecimal.ZERO)))
          .totalAmount(new BigDecimal("0.00"))
          .build();

      assertThat(motivosDe(pedido)).contains("ITEM_UNIT_PRICE_MUST_BE_POSITIVE");
    }
  }

  @Nested
  @DisplayName("Dado um pedido com valor total inconsistente")
  class ValorTotal {

    @Test
    @DisplayName("Quando o total nao bate com a soma, Entao o motivo e TOTAL_AMOUNT_MISMATCH")
    void divergente() {
      final var pedido = pedidoValido().totalAmount(new BigDecimal("999.00")).build();

      assertThat(motivosDe(pedido)).contains("TOTAL_AMOUNT_MISMATCH");
    }

    @Test
    @DisplayName("Quando o total nao bate, Entao o detalhe nomeia o esperado e o recebido")
    void detalheDaDivergencia() {
      final var pedido = pedidoValido().totalAmount(new BigDecimal("999.00")).build();

      final var detalhe = rules.validate(pedido).stream()
          .filter(v -> v.reason().equals("TOTAL_AMOUNT_MISMATCH"))
          .findFirst()
          .orElseThrow()
          .detail();

      assertThat(detalhe).contains("300.00").contains("999.00");
    }

    @Test
    @DisplayName("Quando o total e zero, Entao o motivo e TOTAL_AMOUNT_MUST_BE_POSITIVE")
    void totalZero() {
      final var pedido = pedidoValido().totalAmount(BigDecimal.ZERO).build();

      assertThat(motivosDe(pedido)).contains("TOTAL_AMOUNT_MUST_BE_POSITIVE");
    }
  }

  @Nested
  @DisplayName("Dada uma regra isolada")
  class RegraIsolada {

    @Test
    @DisplayName("Quando invocada diretamente, Entao responde sem Kafka")
    void semKafka() {
      final var regra = new TotalAmountMatchesItemsRule(money);

      assertThat(regra.validate(pedidoValido().build())).isEmpty();
      assertThat(regra.validate(pedidoValido().totalAmount(new BigDecimal("1.00")).build()))
          .isPresent();
    }

    @Test
    @DisplayName("Quando o conjunto e estendido, Entao a etapa que aplica nao muda")
    void extensivel() {
      final var custom = new OrderValidationRules(List.of(
          new OrderIdRequiredRule(),
          order -> Optional.of(
              new OrderValidationRule.Violation("SEMPRE_REJEITA", "regra de teste"))));

      assertThat(custom.validate(pedidoValido().build()))
          .extracting(OrderValidationRule.Violation::reason)
          .containsExactly("SEMPRE_REJEITA");
    }
  }
}
