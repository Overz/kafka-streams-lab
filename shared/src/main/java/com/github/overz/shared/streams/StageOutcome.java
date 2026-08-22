package com.github.overz.shared.streams;

import java.util.Collection;

/**
 * Resultado de uma etapa do pipeline. Rejeicao e valor, nao excecao: uma excecao nao
 * capturada mata a thread da task, enquanto um pedido invalido so precisa mudar de ramo.
 */
public sealed interface StageOutcome<T> permits StageOutcome.Success, StageOutcome.Failure {

  boolean isSuccess();

  T value();

  record Success<T>(T value) implements StageOutcome<T> {
    @Override
    public boolean isSuccess() {
      return true;
    }
  }

  /**
   * payload e o evento original ja serializado. Sem ele a DLQ vira um registro de que
   * algo falhou, sem o dado para reprocessar depois do defeito corrigido.
   */
  record Failure<T>(String reason, String stage, String detail, String payload) implements StageOutcome<T> {
    @Override
    public boolean isSuccess() {
      return false;
    }

    @Override
    public T value() {
      return null;
    }
  }

  static <T> StageOutcome<T> success(final T value) {
    return new Success<>(value);
  }

  static <T> Failure<T> failure(final String reason, final String stage, final String detail) {
    return new Failure<>(reason, stage, detail, null);
  }

  static <T> Failure<T> failures(final String stage,
                                 final Collection<String> reasons,
                                 final String payload) {
    return new Failure<>(String.join(",", reasons), stage, null, payload);
  }
}
