package com.github.overz.orders.rule;

import com.github.overz.shared.event.OrderCreated;

import java.util.Optional;

public final class CreatedAtRequiredRule implements OrderValidationRule {

  @Override
  public Optional<Violation> validate(final OrderCreated order) {
    return order.createdAt() == null
        ? Optional.of(Violation.of("CREATED_AT_REQUIRED"))
        : Optional.empty();
  }
}
