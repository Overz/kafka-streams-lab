package com.github.overz.shared.telemetry;

import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("Span por etapa do pipeline")
class TelemetryTest {

  private InMemorySpanExporter exporter;
  private Telemetry telemetry;

  @BeforeEach
  void montar() {
    exporter = InMemorySpanExporter.create();
    final var sdk = OpenTelemetrySdk.builder()
        .setTracerProvider(SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(exporter))
            .build())
        .build();

    telemetry = new Telemetry("orders", sdk.getTracer("orders"));
  }

  @Nested
  @DisplayName("Dada uma etapa que processa com sucesso")
  class Sucesso {

    @Test
    @DisplayName("Quando a etapa termina, Entao o span encerra com status OK e o id do pedido")
    void spanDeSucesso() {
      final var resultado = telemetry.inStage("validation", "ord-001", () -> "ok");

      assertThat(resultado).isEqualTo("ok");

      final var spans = exporter.getFinishedSpanItems();
      assertThat(spans).singleElement().satisfies(span -> {
        assertThat(span.getName()).isEqualTo("orders.validation");
        assertThat(span.getStatus().getStatusCode()).isEqualTo(StatusCode.OK);
        assertThat(span.getAttributes().asMap().values()).contains("ord-001");
      });
    }
  }

  @Nested
  @DisplayName("Dada uma etapa que rejeita o evento")
  class Rejeicao {

    @Test
    @DisplayName("Quando a etapa marca falha, Entao o span encerra com erro e o motivo")
    void spanDeRejeicao() {
      telemetry.inStage("validation", "ord-002", scope -> {
        scope.rejected("TOTAL_AMOUNT_MISMATCH");
        return null;
      });

      final var spans = exporter.getFinishedSpanItems();
      assertThat(spans).singleElement().satisfies(span -> {
        assertThat(span.getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
        assertThat(span.getAttributes().asMap().values()).contains("TOTAL_AMOUNT_MISMATCH");
      });
    }
  }

  @Nested
  @DisplayName("Dada uma etapa que lanca")
  class Excecao {

    @Test
    @DisplayName("Quando a excecao escapa, Entao o span ainda encerra, com erro")
    void spanEncerraMesmoAssim() {
      assertThatThrownBy(() -> telemetry.inStage("validation", "ord-003", () -> {
        throw new IllegalStateException("falhou");
      })).isInstanceOf(IllegalStateException.class);

      assertThat(exporter.getFinishedSpanItems()).singleElement().satisfies(span ->
          assertThat(span.getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR));
    }
  }
}
