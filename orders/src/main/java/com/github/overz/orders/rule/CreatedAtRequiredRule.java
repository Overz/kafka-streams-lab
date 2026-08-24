package com.github.overz.orders.rule;

import com.github.overz.shared.event.OrderCreated;

import java.util.Optional;

import static com.github.overz.orders.rule.ValidationReason.CREATED_AT_REQUIRED;

public final class CreatedAtRequiredRule implements OrderValidationRule {

  @Override
  public Optional<Violation> validate(final OrderCreated order) {
    return order.createdAt() == null
        ? Optional.of(CREATED_AT_REQUIRED.violation())
        : Optional.empty();
  }
}
