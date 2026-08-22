package com.github.overz.orders.rule;

import com.github.overz.shared.event.OrderCreated;
import com.github.overz.shared.event.OrderItem;

import java.util.List;
import java.util.Optional;

public final class ItemQuantityRule implements OrderValidationRule {

  @Override
  public Optional<Violation> validate(final OrderCreated order) {
    final var items = order.items() == null ? List.<OrderItem>of() : order.items();

    return items.stream().anyMatch(item -> item.quantity() == null || item.quantity() <= 0)
        ? Optional.of(Violation.of("ITEM_QUANTITY_MUST_BE_POSITIVE"))
        : Optional.empty();
  }
}
