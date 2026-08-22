package com.github.overz.orders.rule;

import com.github.overz.shared.event.OrderCreated;

import java.util.Optional;

public final class ItemsRequiredRule implements OrderValidationRule {

  @Override
  public Optional<Violation> validate(final OrderCreated order) {
    return order.items() == null || order.items().isEmpty()
        ? Optional.of(Violation.of("ITEMS_REQUIRED"))
        : Optional.empty();
  }
}
