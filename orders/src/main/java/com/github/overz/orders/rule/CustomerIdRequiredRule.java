package com.github.overz.orders.rule;

import com.github.overz.shared.event.OrderCreated;

import java.util.Optional;

import static com.github.overz.orders.rule.ValidationReason.CUSTOMER_ID_REQUIRED;

public final class CustomerIdRequiredRule implements OrderValidationRule {

  @Override
  public Optional<Violation> validate(final OrderCreated order) {
    return OrderIdRequiredRule.isBlank(order.customerId())
        ? Optional.of(CUSTOMER_ID_REQUIRED.violation())
        : Optional.empty();
  }
}
