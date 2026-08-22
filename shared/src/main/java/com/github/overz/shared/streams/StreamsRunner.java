package com.github.overz.shared.streams;

import com.github.overz.shared.config.KafkaConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

// Ciclo de vida de uma aplicacao Streams do lab: monta, inicia, segura o main e fecha
// limpo no sinal do sistema.
//
// Recebe Topology, Properties e os dois contratos de erro do proprio Kafka Streams --
// nenhum tipo de dominio. Por isso serve aos dois dominios sem saber de nenhum.
@Slf4j
@RequiredArgsConstructor
public final class StreamsRunner implements AutoCloseable {

  private final String application;
  private final Topology topology;
  private final Properties properties;
  private final StreamsUncaughtExceptionHandler exceptionHandler;
  private final Duration shutdownTimeout;

  private final CountDownLatch stopped = new CountDownLatch(1);
  private KafkaStreams streams;

  public static StreamsRunner of(final String application,
                                 final TopologySupplier supplier,
                                 final KafkaConfig kafka) {
    return new StreamsRunner(
        application,
        supplier.topology(),
        new StreamsProperties(kafka).toProperties(),
        new ReplaceThreadExceptionHandler(application),
        Duration.ofSeconds(30));
  }

  public void run() {
    start(StateTransitionObserver.NONE);
    await();
  }

  /**
   * O observador entra aqui, e nao no construtor: quem observa o ciclo de vida
   * normalmente precisa do proprio runner (a ponte de metricas le runner::metrics), e
   * exigi-lo na construcao criaria um ciclo que so um campo mutavel resolveria.
   */
  public void start(final StateTransitionObserver observer) {
    log.info("iniciando aplicacao streams, aplicacao='{}', applicationId='{}', brokers='{}'",
        application,
        properties.get("application.id"),
        properties.get("bootstrap.servers"));

    log.debug("topologia descrita, aplicacao='{}'\n{}", application, topology.describe());

    streams = new KafkaStreams(topology, properties);
    streams.setStateListener(new LoggingStateListener(application, observer));
    streams.setUncaughtExceptionHandler(exceptionHandler);

    // Registrado antes do start: um Ctrl+C entre o start e o registro deixaria a
    // aplicacao morrer no meio de um commit e o proximo boot reprocessaria.
    Runtime.getRuntime().addShutdownHook(new Thread(this::close, application + "-shutdown"));

    streams.start();
    log.info("aplicacao streams iniciada, aplicacao='{}'", application);
  }

  // Metricas nativas da instancia, para a ponte que as publica no OpenTelemetry.
  public Map<MetricName, ? extends Metric> metrics() {
    return streams == null ? Map.of() : streams.metrics();
  }

  public void await() {
    try {
      stopped.await();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      log.warn("espera interrompida, aplicacao='{}'", application);
    }
  }

  @Override
  public void close() {
    if (streams == null) {
      stopped.countDown();
      return;
    }

    log.info("encerrando aplicacao streams, aplicacao='{}', estado='{}', timeout='{}'",
        application, streams.state(), shutdownTimeout);

    final var fechou = streams.close(shutdownTimeout);

    log.info("aplicacao streams encerrada, aplicacao='{}', limpo={}, estadoFinal='{}'",
        application, fechou, streams.state());

    stopped.countDown();
  }
}
