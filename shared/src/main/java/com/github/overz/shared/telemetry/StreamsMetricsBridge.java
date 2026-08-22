package com.github.overz.shared.telemetry;

import com.github.overz.shared.streams.StateTransitionObserver;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleGauge;
import io.opentelemetry.api.metrics.ObservableDoubleMeasurement;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.streams.KafkaStreams;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

// Publica as metricas nativas do Kafka Streams como gauges OTel, para que caiam no mesmo
// backend das metricas JMX dos brokers e possam ser comparadas no mesmo painel.
//
// Registra ao entrar em RUNNING, e nao no start: no start a aplicacao ainda esta em
// REBALANCING e as metricas de thread, task e store nem existem -- so as de client. E
// reobserva a cada nova transicao para RUNNING, porque um rebalanceamento cria metricas
// para as tasks que a instancia acabou de assumir.
//
// Recebe um Supplier do mapa, e nao a instancia de KafkaStreams: a ponte fica testavel sem
// broker e sem topologia.
@Slf4j
@RequiredArgsConstructor
public final class StreamsMetricsBridge implements StateTransitionObserver, AutoCloseable {

  private static final String PREFIX = "kafka.streams.";
  private static final String GROUP_SUFFIX = "-metrics";

  private final Meter meter;
  private final Supplier<Map<MetricName, ? extends Metric>> metrics;
  private final Map<String, ObservableDoubleGauge> gauges = new ConcurrentHashMap<>();

  @Override
  public void onTransition(final KafkaStreams.State previous, final KafkaStreams.State current) {
    if (current == KafkaStreams.State.RUNNING) {
      register();
    }
  }

  // Idempotente: chamada a cada RUNNING, cria instrumento apenas para o que ainda nao tem.
  public void register() {
    final var antes = gauges.size();
    var ignoradas = 0;

    for (final var entry : metrics.get().entrySet()) {
      if (!(entry.getValue().metricValue() instanceof Number)) {
        // Metrica de texto, como o estado da thread: nao vira gauge.
        ignoradas++;
        continue;
      }

      final var name = entry.getKey();
      gauges.computeIfAbsent(instrumentOf(name), instrument -> create(instrument, name));
    }

    log.info("metricas do kafka streams publicadas no opentelemetry, instrumentos={}, novos={}, ignoradas={}",
        gauges.size(), gauges.size() - antes, ignoradas);
  }

  private ObservableDoubleGauge create(final String instrument, final MetricName name) {
    return meter.gaugeBuilder(instrument)
        .setDescription(name.description())
        // O valor e lido na coleta, nao agora: um gauge que guardasse o valor do registro
        // reportaria para sempre o numero do instante do boot.
        .buildWithCallback(measurement -> observe(name.group(), name.name(), measurement));
  }

  /**
   * Um instrumento por grupo e nome; cada thread, task ou store vira uma serie propria,
   * distinguida pelos atributos. Registrar um gauge por MetricName criaria tres
   * instrumentos "process-rate" com o mesmo nome quando ha tres threads, e o SDK do
   * OpenTelemetry recusa instrumentos duplicados.
   */
  private void observe(final String group,
                       final String metric,
                       final ObservableDoubleMeasurement measurement) {

    metrics.get().forEach((name, value) -> {
      if (name.group().equals(group)
          && name.name().equals(metric)
          && value.metricValue() instanceof Number number) {
        measurement.record(number.doubleValue(), attributesOf(name));
      }
    });
  }

  /**
   * O grupo entra no nome do instrumento. Sem ele, "process-rate" de
   * stream-thread-metrics, stream-task-metrics e stream-processor-node-metrics colidiriam
   * num unico "kafka.streams.process-rate" com as tres semanticas misturadas.
   */
  private static String instrumentOf(final MetricName name) {
    final var group = name.group().endsWith(GROUP_SUFFIX)
        ? name.group().substring(0, name.group().length() - GROUP_SUFFIX.length())
        : name.group();

    return PREFIX + group + "." + name.name();
  }

  private static Attributes attributesOf(final MetricName name) {
    final var builder = Attributes.builder().put("metric.group", name.group());
    name.tags().forEach((key, value) -> builder.put(AttributeKey.stringKey(key), value));
    return builder.build();
  }

  @Override
  public void close() {
    gauges.values().forEach(ObservableDoubleGauge::close);
    gauges.clear();
  }
}
