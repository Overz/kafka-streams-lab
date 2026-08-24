package com.github.overz.orders.rule;

import com.github.overz.shared.event.OrderCreated;

import java.util.Optional;

import static com.github.overz.orders.rule.ValidationReason.CURRENCY_REQUIRED;

public final class CurrencyRequiredRule implements OrderValidationRule {

  @Override
  public Optional<Violation> validate(final OrderCreated order) {
    return OrderIdRequiredRule.isBlank(order.currency())
        ? Optional.of(CURRENCY_REQUIRED.violation())
        : Optional.empty();
  }
}
