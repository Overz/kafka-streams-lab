package com.github.overz.orders.rule;

import com.github.overz.shared.event.OrderCreated;

import java.util.Optional;

import static com.github.overz.orders.rule.ValidationReason.ORDER_ID_REQUIRED;

public final class OrderIdRequiredRule implements OrderValidationRule {

  @Override
  public Optional<Violation> validate(final OrderCreated order) {
    return isBlank(order.orderId()) ? Optional.of(ORDER_ID_REQUIRED.violation()) : Optional.empty();
  }

  static boolean isBlank(final String value) {
    return value == null || value.isBlank();
  }
}
