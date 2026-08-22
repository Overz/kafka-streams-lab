package com.github.overz.orders.rule;

import com.github.overz.shared.event.Money;
import com.github.overz.shared.event.OrderCreated;
import com.github.overz.shared.event.OrderItem;
import lombok.RequiredArgsConstructor;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

/**
 * A regra do documento: totalAmount tem de bater com a soma de quantity x unitPrice.
 * A conta e feita em BigDecimal pela classe Money -- em double a comparacao falharia de
 * forma intermitente, so com certos valores.
 */
@RequiredArgsConstructor
public final class TotalAmountMatchesItemsRule implements OrderValidationRule {

  private final Money money;

  @Override
  public Optional<Violation> validate(final OrderCreated order) {
    if (order.items() == null || order.items().isEmpty() || order.totalAmount() == null) {
      // Ausencia de item ou de total ja tem regra propria; aqui seria um segundo motivo
      // para o mesmo defeito.
      return Optional.empty();
    }

    final var expected = money.sum(subtotals(order.items()));

    if (money.isEqual(expected, order.totalAmount())) {
      return Optional.empty();
    }

    return Optional.of(new Violation("TOTAL_AMOUNT_MISMATCH",
        "esperado=%s recebido=%s".formatted(expected, money.normalize(order.totalAmount()))));
  }

  private List<BigDecimal> subtotals(final List<OrderItem> items) {
    return items.stream()
        .filter(item -> item.unitPrice() != null && item.quantity() != null)
        .map(item -> money.multiply(item.unitPrice(), item.quantity()))
        .toList();
  }
}
