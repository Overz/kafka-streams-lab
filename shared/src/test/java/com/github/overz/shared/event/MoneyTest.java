package com.github.overz.shared.event;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Aritmetica monetaria")
class MoneyTest {

  private final Money money = Money.brl();

  @Nested
  @DisplayName("Dado valores com casas decimais")
  class Soma {

    @Test
    @DisplayName("Quando somados 0.10 e 0.20, Entao o resultado e igual a 0.30")
    void semErroDePontoFlutuante() {
      final var total = money.sum(List.of(new BigDecimal("0.10"), new BigDecimal("0.20")));

      assertThat(money.isEqual(total, new BigDecimal("0.30"))).isTrue();
    }

    @Test
    @DisplayName("Quando somada uma lista vazia, Entao o resultado e zero")
    void listaVazia() {
      assertThat(money.isEqual(money.sum(List.of()), BigDecimal.ZERO)).isTrue();
    }
  }

  @Nested
  @DisplayName("Dado valores em escalas diferentes")
  class Comparacao {

    @Test
    @DisplayName("Quando comparados 300.00 e 300.0, Entao sao considerados iguais")
    void escalasDiferentes() {
      assertThat(money.isEqual(new BigDecimal("300.00"), new BigDecimal("300.0"))).isTrue();
    }

    @Test
    @DisplayName("Quando comparados 300.00 e 300.01, Entao sao considerados diferentes")
    void valoresDiferentes() {
      assertThat(money.isEqual(new BigDecimal("300.00"), new BigDecimal("300.01"))).isFalse();
    }

    @Test
    @DisplayName("Quando um dos lados e nulo, Entao nao sao considerados iguais")
    void nulo() {
      assertThat(money.isEqual(null, new BigDecimal("1.00"))).isFalse();
      assertThat(money.isEqual(new BigDecimal("1.00"), null)).isFalse();
    }
  }

  @Nested
  @DisplayName("Dado um item de pedido")
  class Multiplicacao {

    @Test
    @DisplayName("Quando 2 unidades de 150.00, Entao o subtotal e 300.00")
    void subtotal() {
      final var subtotal = money.multiply(new BigDecimal("150.00"), 2);

      assertThat(money.isEqual(subtotal, new BigDecimal("300.00"))).isTrue();
    }

    @Test
    @DisplayName("Quando 3 unidades de 0.335, Entao o subtotal arredonda para 1.01")
    void arredondamentoHalfUp() {
      final var subtotal = money.multiply(new BigDecimal("0.335"), 3);

      assertThat(money.isEqual(subtotal, new BigDecimal("1.01"))).isTrue();
    }
  }

  @Nested
  @DisplayName("Dado uma comparacao de ordem")
  class Ordem {

    @Test
    @DisplayName("Quando 2500.00 comparado com o limite 2000.00, Entao e maior")
    void maiorQue() {
      assertThat(money.isGreaterThan(new BigDecimal("2500.00"), new BigDecimal("2000.00"))).isTrue();
    }

    @Test
    @DisplayName("Quando 2000.00 comparado com o limite 2000.00, Entao nao e maior")
    void igualNaoEMaior() {
      assertThat(money.isGreaterThan(new BigDecimal("2000.00"), new BigDecimal("2000.00"))).isFalse();
    }
  }
}
