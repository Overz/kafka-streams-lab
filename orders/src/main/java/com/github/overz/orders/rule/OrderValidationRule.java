package com.github.overz.orders.rule;

import com.github.overz.shared.event.OrderCreated;

import java.util.Optional;

/**
 * Uma regra, um motivo. Acrescentar regra nao mexe em quem as aplica, e cada uma pode ser
 * exercitada sozinha, sem Kafka.
 */
@FunctionalInterface
public interface OrderValidationRule {

  record Violation(String reason, String detail) {

    public static Violation of(final String reason) {
      return new Violation(reason, null);
    }
  }

  Optional<Violation> validate(OrderCreated order);
}
