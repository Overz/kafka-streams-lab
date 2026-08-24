package com.github.overz.orders.rule;

import com.github.overz.orders.rule.OrderValidationRule.Violation;

/**
 * O vocabulario fechado de motivos de rejeicao na validacao.
 *
 * O name() de cada constante viaja para fora do processo: vai concatenado em
 * DeadLetter.reason e no header x-error-reason. Renomear constante quebra consumidor a
 * jusante -- acrescentar, nao.
 */
public enum ValidationReason {

  ORDER_ID_REQUIRED,
  CUSTOMER_ID_REQUIRED,
  CURRENCY_REQUIRED,
  CREATED_AT_REQUIRED,
  ITEMS_REQUIRED,
  ITEM_PRODUCT_ID_REQUIRED,
  ITEM_QUANTITY_MUST_BE_POSITIVE,
  ITEM_UNIT_PRICE_MUST_BE_POSITIVE,
  TOTAL_AMOUNT_MUST_BE_POSITIVE,
  TOTAL_AMOUNT_MISMATCH;

  /** Motivo sem detalhe: o proprio nome da constante ja diz o que faltou. */
  public Violation violation() {
    return new Violation(this, null);
  }

  /** Motivo com detalhe de runtime, para quando o nome sozinho nao localiza o defeito. */
  public Violation detailed(final String detail) {
    return new Violation(this, detail);
  }
}
