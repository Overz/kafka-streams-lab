package com.github.overz.shared.streams;

import com.github.overz.shared.event.OrderCreated;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Resultado de etapa como valor")
class StageOutcomeTest {

  @Nested
  @DisplayName("Dado uma etapa que aceita o evento")
  class Sucesso {

    @Test
    @DisplayName("Quando devolve Success, Entao o valor esta acessivel e nao ha falha")
    void success() {
      final StageOutcome<String> resultado = StageOutcome.success("ok");

      assertThat(resultado.isSuccess()).isTrue();
      assertThat(resultado.value()).isEqualTo("ok");
    }
  }

  @Nested
  @DisplayName("Dado uma etapa que rejeita o evento")
  class Falha {

    @Test
    @DisplayName("Quando devolve Failure, Entao carrega motivo, etapa e detalhe")
    void failure() {
      final StageOutcome<OrderCreated> resultado =
          StageOutcome.failure("TOTAL_AMOUNT_MISMATCH", "validation", "esperado=300.00 recebido=999.00");

      assertThat(resultado.isSuccess()).isFalse();

      final var falha = (StageOutcome.Failure<OrderCreated>) resultado;
      assertThat(falha.reason()).isEqualTo("TOTAL_AMOUNT_MISMATCH");
      assertThat(falha.stage()).isEqualTo("validation");
      assertThat(falha.detail()).contains("999.00");
    }

    @Test
    @DisplayName("Quando ha varias falhas, Entao a fabrica agrega os motivos numa so")
    void agregacao() {
      final var falha = StageOutcome.<OrderCreated>failures(
          "validation", List.of("ORDER_ID_REQUIRED", "CURRENCY_REQUIRED"), "{\"orderId\":null}");

      assertThat(falha.reason()).isEqualTo("ORDER_ID_REQUIRED,CURRENCY_REQUIRED");
      assertThat(falha.stage()).isEqualTo("validation");
    }

    @Test
    @DisplayName("Quando ha falha, Entao o payload original acompanha para permitir reprocessamento")
    void carregaPayload() {
      final var falha = StageOutcome.<OrderCreated>failures(
          "validation", List.of("ORDER_ID_REQUIRED"), "{\"customerId\":\"cus-001\"}");

      assertThat(falha.payload()).contains("cus-001");
    }
  }

  @Nested
  @DisplayName("Dado um switch sobre o resultado")
  class Exaustividade {

    @Test
    @DisplayName("Quando cobre Success e Failure, Entao compila sem ramo default")
    void patternMatching() {
      final StageOutcome<String> resultado = StageOutcome.success("valor");

      final var descricao = switch (resultado) {
        case StageOutcome.Success<String> s -> "ok:" + s.value();
        case StageOutcome.Failure<String> f -> "erro:" + f.reason();
      };

      assertThat(descricao).isEqualTo("ok:valor");
    }
  }
}
