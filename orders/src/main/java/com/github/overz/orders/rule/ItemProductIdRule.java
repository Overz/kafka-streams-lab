package com.github.overz.orders.rule;

import com.github.overz.shared.event.OrderCreated;
import com.github.overz.shared.event.OrderItem;

import java.util.List;
import java.util.Optional;

public final class ItemProductIdRule implements OrderValidationRule {

  @Override
  public Optional<Violation> validate(final OrderCreated order) {
    final var items = order.items() == null ? List.<OrderItem>of() : order.items();

    return items.stream().anyMatch(item -> OrderIdRequiredRule.isBlank(item.productId()))
        ? Optional.of(Violation.of("ITEM_PRODUCT_ID_REQUIRED"))
        : Optional.empty();
  }
}
