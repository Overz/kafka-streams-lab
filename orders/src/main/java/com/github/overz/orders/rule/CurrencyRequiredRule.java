package com.github.overz.orders.rule;

import com.github.overz.shared.event.OrderCreated;

import java.util.Optional;

public final class CurrencyRequiredRule implements OrderValidationRule {

  @Override
  public Optional<Violation> validate(final OrderCreated order) {
    return OrderIdRequiredRule.isBlank(order.currency())
        ? Optional.of(Violation.of("CURRENCY_REQUIRED"))
        : Optional.empty();
  }
}
