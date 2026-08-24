package com.github.overz.orders.rule;

import com.github.overz.shared.event.Money;
import com.github.overz.shared.event.OrderCreated;
import com.github.overz.shared.event.OrderItem;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Optional;

import static com.github.overz.orders.rule.ValidationReason.ITEM_UNIT_PRICE_MUST_BE_POSITIVE;

@RequiredArgsConstructor
public final class ItemUnitPriceRule implements OrderValidationRule {

  private final Money money;

  @Override
  public Optional<Violation> validate(final OrderCreated order) {
    final var items = order.items() == null ? List.<OrderItem>of() : order.items();

    return items.stream().anyMatch(item -> !money.isPositive(item.unitPrice()))
        ? Optional.of(ITEM_UNIT_PRICE_MUST_BE_POSITIVE.violation())
        : Optional.empty();
  }
}
