package com.github.overz.shared.event;

import lombok.RequiredArgsConstructor;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Collection;
import java.util.Objects;

/**
 * Instancia, e nao utilitario estatico: a escala e o modo de arredondamento sao decisao
 * de configuracao e cada dominio recebe a sua por construtor.
 */
@RequiredArgsConstructor
public final class Money {

  private final int scale;
  private final RoundingMode rounding;

  public static Money brl() {
    return new Money(2, RoundingMode.HALF_UP);
  }

  public BigDecimal normalize(final BigDecimal value) {
    return value == null ? null : value.setScale(scale, rounding);
  }

  public BigDecimal zero() {
    return BigDecimal.ZERO.setScale(scale, rounding);
  }

  public BigDecimal sum(final Collection<BigDecimal> values) {
    return values.stream()
      .filter(Objects::nonNull)
      .reduce(BigDecimal.ZERO, BigDecimal::add)
      .setScale(scale, rounding);
  }

  public BigDecimal multiply(final BigDecimal unit, final int quantity) {
    return unit == null ? null : unit.multiply(BigDecimal.valueOf(quantity)).setScale(scale, rounding);
  }

  public BigDecimal divide(final BigDecimal dividend, final long divisor) {
    if (dividend == null || divisor == 0L) {
      return null;
    }
    return dividend.divide(BigDecimal.valueOf(divisor), scale, rounding);
  }

  // compareTo e nao equals: 300.00 e 300.0 sao o mesmo dinheiro com escalas diferentes,
  // e equals() de BigDecimal considera a escala parte da identidade.
  public boolean isEqual(final BigDecimal left, final BigDecimal right) {
    return left != null && right != null && left.compareTo(right) == 0;
  }

  public boolean isGreaterThan(final BigDecimal left, final BigDecimal right) {
    return left != null && right != null && left.compareTo(right) > 0;
  }

  public boolean isPositive(final BigDecimal value) {
    return value != null && value.signum() > 0;
  }
}
