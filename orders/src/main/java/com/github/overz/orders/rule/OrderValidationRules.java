package com.github.overz.orders.rule;

import com.github.overz.shared.event.Money;
import com.github.overz.shared.event.OrderCreated;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Optional;

/**
 * Aplica todas as regras e devolve todas as violacoes.
 *
 * Todas, e nao ate a primeira: parar no primeiro erro obrigaria quem publicou o pedido a
 * corrigir um campo por vez, descobrindo o proximo defeito so na tentativa seguinte.
 */
@RequiredArgsConstructor
public final class OrderValidationRules {

  private final List<OrderValidationRule> rules;

  public static OrderValidationRules standard(final Money money) {
    return new OrderValidationRules(List.of(
        new OrderIdRequiredRule(),
        new CustomerIdRequiredRule(),
        new CurrencyRequiredRule(),
        new CreatedAtRequiredRule(),
        new ItemsRequiredRule(),
        new ItemProductIdRule(),
        new ItemQuantityRule(),
        new ItemUnitPriceRule(money),
        new TotalAmountPositiveRule(money),
        new TotalAmountMatchesItemsRule(money)));
  }

  public List<OrderValidationRule.Violation> validate(final OrderCreated order) {
    return rules.stream()
        .map(rule -> rule.validate(order))
        .flatMap(Optional::stream)
        .toList();
  }
}
