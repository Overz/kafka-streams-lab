package com.github.overz.orders.rule;

import com.github.overz.shared.event.OrderCreated;

import java.util.Optional;

public final class CustomerIdRequiredRule implements OrderValidationRule {

  @Override
  public Optional<Violation> validate(final OrderCreated order) {
    return OrderIdRequiredRule.isBlank(order.customerId())
        ? Optional.of(Violation.of("CUSTOMER_ID_REQUIRED"))
        : Optional.empty();
  }
}
