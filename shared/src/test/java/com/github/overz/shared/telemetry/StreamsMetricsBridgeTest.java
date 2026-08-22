package com.github.overz.shared.telemetry;

import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Ponte entre as metricas do Kafka Streams e o OpenTelemetry")
class StreamsMetricsBridgeTest {

  private record FixedMetric(MetricName metricName, Object metricValue) implements Metric {
  }

  private final Map<MetricName, Metric> metricas = new LinkedHashMap<>();

  private InMemoryMetricReader reader;
  private StreamsMetricsBridge bridge;

  @BeforeEach
  void montar() {
    reader = InMemoryMetricReader.create();
    final var provider = SdkMeterProvider.builder().registerMetricReader(reader).build();
    bridge = new StreamsMetricsBridge(provider.get("teste"), () -> metricas);
  }

  private void metrica(final String grupo, final String nome, final Object valor,
                       final Map<String, String> tags) {
    metricas.put(new MetricName(nome, grupo, "metrica de teste", tags), new FixedMetric(null, valor));
  }

  @Nested
  @DisplayName("Dada uma metrica numerica")
  class Numericas {

    @Test
    @DisplayName("Quando coletada, Entao vira gauge nomeado pelo grupo e pelo nome")
    void viraGauge() {
      metrica("stream-thread-metrics", "process-rate", 42.5d, Map.of("thread-id", "t-1"));

      bridge.register();

      assertThat(reader.collectAllMetrics()).singleElement().satisfies(m -> {
        // O grupo entra no nome; o sufixo "-metrics" e redundante e sai.
        assertThat(m.getName()).isEqualTo("kafka.streams.stream-thread.process-rate");
        assertThat(m.getDoubleGaugeData().getPoints()).singleElement().satisfies(p -> {
          assertThat(p.getValue()).isEqualTo(42.5d);
          assertThat(p.getAttributes().asMap().values()).contains("t-1");
        });
      });
    }

    @Test
    @DisplayName("Quando o valor muda entre coletas, Entao a leitura seguinte reflete o atual")
    void valorLidoNaColeta() {
      metrica("stream-metrics", "records-lag", 10.0d, Map.of());
      bridge.register();

      assertThat(valorUnico()).isEqualTo(10.0d);

      metrica("stream-metrics", "records-lag", 99.0d, Map.of());

      assertThat(valorUnico()).isEqualTo(99.0d);
    }
  }

  @Nested
  @DisplayName("Dado o mesmo nome de metrica em contextos diferentes")
  class SemColisao {

    @Test
    @DisplayName("Quando o nome se repete em grupos diferentes, Entao cada grupo tem seu instrumento")
    void grupoNoNome() {
      metrica("stream-thread-metrics", "process-rate", 1.0d, Map.of());
      metrica("stream-task-metrics", "process-rate", 2.0d, Map.of());

      bridge.register();

      assertThat(reader.collectAllMetrics())
          .extracting(MetricData::getName)
          .containsExactlyInAnyOrder(
              "kafka.streams.stream-thread.process-rate",
              "kafka.streams.stream-task.process-rate");
    }

    @Test
    @DisplayName("Quando varias threads tem a mesma metrica, Entao ha um instrumento e uma serie por thread")
    void umInstrumentoVariasSeries() {
      metrica("stream-thread-metrics", "process-rate", 1.0d, Map.of("thread-id", "t-1"));
      metrica("stream-thread-metrics", "process-rate", 2.0d, Map.of("thread-id", "t-2"));
      metrica("stream-thread-metrics", "process-rate", 3.0d, Map.of("thread-id", "t-3"));

      bridge.register();

      assertThat(reader.collectAllMetrics()).singleElement().satisfies(m -> {
        assertThat(m.getName()).isEqualTo("kafka.streams.stream-thread.process-rate");
        assertThat(m.getDoubleGaugeData().getPoints())
            .extracting(p -> p.getValue())
            .containsExactlyInAnyOrder(1.0d, 2.0d, 3.0d);
      });
    }
  }

  @Nested
  @DisplayName("Dada uma metrica que nao vira gauge")
  class Ignoradas {

    @Test
    @DisplayName("Quando o valor nao e numerico, Entao e ignorada sem lancar")
    void naoNumerica() {
      metrica("stream-metrics", "state", "RUNNING", Map.of());
      metrica("stream-metrics", "commit-rate", 1.0d, Map.of());

      bridge.register();

      assertThat(reader.collectAllMetrics())
          .extracting(MetricData::getName)
          .containsExactly("kafka.streams.stream.commit-rate");
    }
  }

  @Nested
  @DisplayName("Dado o ciclo de vida da aplicacao")
  class CicloDeVida {

    @Test
    @DisplayName("Quando ainda nao esta em RUNNING, Entao nada e registrado")
    void antesDoRunning() {
      metrica("stream-metrics", "commit-rate", 1.0d, Map.of());

      bridge.onTransition(KafkaStreams.State.CREATED, KafkaStreams.State.REBALANCING);

      assertThat(reader.collectAllMetrics()).isEmpty();
    }

    @Test
    @DisplayName("Quando entra em RUNNING, Entao as metricas passam a ser publicadas")
    void aoEntrarEmRunning() {
      metrica("stream-metrics", "commit-rate", 1.0d, Map.of());

      bridge.onTransition(KafkaStreams.State.REBALANCING, KafkaStreams.State.RUNNING);

      assertThat(reader.collectAllMetrics()).hasSize(1);
    }

    @Test
    @DisplayName("Quando um rebalanceamento cria metricas novas, Entao o RUNNING seguinte as publica")
    void metricasNovasAposRebalanceamento() {
      metrica("stream-metrics", "commit-rate", 1.0d, Map.of());
      bridge.onTransition(KafkaStreams.State.REBALANCING, KafkaStreams.State.RUNNING);
      assertThat(reader.collectAllMetrics()).hasSize(1);

      // A task so passa a existir depois que a instancia a assume num rebalanceamento.
      metrica("stream-task-metrics", "process-rate", 5.0d, Map.of("task-id", "0_1"));
      bridge.onTransition(KafkaStreams.State.REBALANCING, KafkaStreams.State.RUNNING);

      assertThat(reader.collectAllMetrics())
          .extracting(MetricData::getName)
          .containsExactlyInAnyOrder(
              "kafka.streams.stream.commit-rate",
              "kafka.streams.stream-task.process-rate");
    }

    @Test
    @DisplayName("Quando RUNNING acontece de novo, Entao o instrumento nao e duplicado")
    void registroIdempotente() {
      metrica("stream-metrics", "commit-rate", 1.0d, Map.of());

      bridge.onTransition(KafkaStreams.State.REBALANCING, KafkaStreams.State.RUNNING);
      bridge.onTransition(KafkaStreams.State.REBALANCING, KafkaStreams.State.RUNNING);

      assertThat(reader.collectAllMetrics()).hasSize(1);
    }

    @Test
    @DisplayName("Quando a ponte e fechada, Entao nada mais e publicado")
    void fechamento() {
      metrica("stream-metrics", "commit-rate", 1.0d, Map.of());
      bridge.register();

      bridge.close();

      assertThat(reader.collectAllMetrics()).isEmpty();
    }
  }

  private double valorUnico() {
    return reader.collectAllMetrics().iterator().next()
        .getDoubleGaugeData().getPoints().iterator().next().getValue();
  }
}
