package com.github.overz.orders.rule;

import com.github.overz.shared.event.Money;
import com.github.overz.shared.event.OrderCreated;
import lombok.RequiredArgsConstructor;

import java.util.Optional;

import static com.github.overz.orders.rule.ValidationReason.TOTAL_AMOUNT_MUST_BE_POSITIVE;

@RequiredArgsConstructor
public final class TotalAmountPositiveRule implements OrderValidationRule {

  private final Money money;

  @Override
  public Optional<Violation> validate(final OrderCreated order) {
    return money.isPositive(order.totalAmount())
        ? Optional.empty()
        : Optional.of(TOTAL_AMOUNT_MUST_BE_POSITIVE.violation());
  }
}
