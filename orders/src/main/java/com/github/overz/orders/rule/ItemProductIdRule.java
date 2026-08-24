package com.github.overz.orders.rule;

import com.github.overz.shared.event.OrderCreated;
import com.github.overz.shared.event.OrderItem;

import java.util.List;
import java.util.Optional;

import static com.github.overz.orders.rule.ValidationReason.ITEM_PRODUCT_ID_REQUIRED;

public final class ItemProductIdRule implements OrderValidationRule {

  @Override
  public Optional<Violation> validate(final OrderCreated order) {
    final var items = order.items() == null ? List.<OrderItem>of() : order.items();

    return items.stream().anyMatch(item -> OrderIdRequiredRule.isBlank(item.productId()))
        ? Optional.of(ITEM_PRODUCT_ID_REQUIRED.violation())
        : Optional.empty();
  }
}
