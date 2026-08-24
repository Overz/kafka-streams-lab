package com.github.overz.orders.rule;

import com.github.overz.shared.event.OrderCreated;

import java.util.Optional;

/**
 * Uma regra, um motivo. Acrescentar regra nao mexe em quem as aplica, e cada uma pode ser
 * exercitada sozinha, sem Kafka.
 */
@FunctionalInterface
public interface OrderValidationRule {

  /**
   * O motivo e tipado; o detalhe e texto de runtime, e so existe quando o nome do motivo
   * sozinho nao localiza o defeito. Construir pelo enum: ORDER_ID_REQUIRED.violation().
   */
  record Violation(ValidationReason reason, String detail) {
  }

  Optional<Violation> validate(OrderCreated order);
}
