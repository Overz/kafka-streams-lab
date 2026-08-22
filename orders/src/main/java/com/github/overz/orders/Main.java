package com.github.overz.orders;

import com.github.overz.orders.mapper.OrdersMapperImpl;
import com.github.overz.orders.pipeline.CompletionPipeline;
import com.github.overz.orders.pipeline.DecisionPipeline;
import com.github.overz.orders.pipeline.FraudRequestPipeline;
import com.github.overz.orders.pipeline.ValidationPipeline;
import com.github.overz.orders.rule.OrderValidationRules;
import com.github.overz.orders.stage.CompletionProcessor;
import com.github.overz.orders.stage.DeadLetterProcessor;
import com.github.overz.orders.stage.DecisionJoiner;
import com.github.overz.orders.stage.FraudRequestProcessor;
import com.github.overz.orders.stage.ValidationProcessor;
import com.github.overz.shared.config.ConfigServerClient;
import com.github.overz.shared.event.DeadLetter;
import com.github.overz.shared.event.FraudAnalysisCompleted;
import com.github.overz.shared.event.Money;
import com.github.overz.shared.event.OrderCreated;
import com.github.overz.shared.event.OrderDecided;
import com.github.overz.shared.event.OrderForFraudAnalysis;
import com.github.overz.shared.event.OrderProcessed;
import com.github.overz.shared.event.OrderValidated;
import com.github.overz.shared.serde.JsonSerdes;
import com.github.overz.shared.streams.Pipeline;
import com.github.overz.shared.streams.PipelineTopology;
import com.github.overz.shared.streams.StreamsRunner;
import com.github.overz.shared.telemetry.OpenTelemetryBootstrap;
import com.github.overz.shared.telemetry.StreamsMetricsBridge;
import com.github.overz.shared.telemetry.Telemetry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;

import java.math.RoundingMode;
import java.time.Clock;
import java.util.List;

// O unico lugar do modulo que conhece implementacoes concretas. As pipelines recebem
// ProcessorSupplier e ValueJoiner; quem decide o que ha por tras deles e este arquivo.
@Slf4j
public final class Main {

  public static void main(final String[] args) {
    final var config = ConfigServerClient.shared().fetch(OrdersConfig.class);
    final var clock = Clock.systemUTC();
    final var application = ConfigServerClient.shared().application();

    log.info("iniciando dominio orders, applicationId='{}', brokers='{}', topicos={}",
        config.kafka().applicationId(), config.kafka().bootstrapServers(), config.topics());

    final var openTelemetry = new OpenTelemetryBootstrap(application, config.telemetry()).start();
    final var telemetry = new Telemetry(application, openTelemetry);

    final var runner = StreamsRunner.of(application,
        new PipelineTopology(pipelines(config, clock, telemetry)),
        config.kafka());

    // A ponte observa o ciclo de vida: as metricas de thread, task e store so existem
    // depois do RUNNING, e cada rebalanceamento cria outras tantas.
    runner.start(new StreamsMetricsBridge(openTelemetry.getMeter(application), runner::metrics));
    runner.await();
  }

  private static List<Pipeline> pipelines(final OrdersConfig config,
                                          final Clock clock,
                                          final Telemetry telemetry) {
    final var serdes = new JsonSerdes();
    final var key = Serdes.String();
    final var mapper = new OrdersMapperImpl();
    final var money = new Money(config.validation().moneyScale(), RoundingMode.HALF_UP);
    final var topics = config.topics();

    return List.of(
        ValidationPipeline.builder()
            .input(topics.orderCreated())
            .validOutput(topics.orderValidated())
            .deadLetterOutput(topics.deadLetter())
            .keySerde(key)
            .inputSerde(serdes.of(OrderCreated.class))
            .validSerde(serdes.of(OrderValidated.class))
            .deadLetterSerde(serdes.of(DeadLetter.class))
            // Supplier inline: cada task recebe a sua instancia, e fica visivel aqui que
            // e uma nova por task, e nao um processor compartilhado entre threads.
            .validation(() -> new ValidationProcessor(
                OrderValidationRules.standard(money), mapper,
                serdes.of(OrderCreated.class).serializer(), clock, telemetry))
            .deadLetter(() -> new DeadLetterProcessor(clock, config.deadLetter().maxPayloadLength()))
            .build(),

        FraudRequestPipeline.builder()
            .input(topics.orderValidated())
            .output(topics.fraudRequested())
            .keySerde(key)
            .inputSerde(serdes.of(OrderValidated.class))
            .outputSerde(serdes.of(OrderForFraudAnalysis.class))
            .projection(() -> new FraudRequestProcessor(mapper, telemetry))
            .build(),

        DecisionPipeline.builder()
            .analysisInput(topics.analysisCompleted())
            .orderStateInput(topics.fraudRequested())
            .stateStore(config.stores().orderState())
            .approvedOutput(topics.orderApproved())
            .rejectedOutput(topics.orderRejected())
            .keySerde(key)
            .analysisSerde(serdes.of(FraudAnalysisCompleted.class))
            .orderStateSerde(serdes.of(OrderForFraudAnalysis.class))
            .decidedSerde(serdes.of(OrderDecided.class))
            .decision(new DecisionJoiner(mapper, clock, telemetry))
            .build(),

        CompletionPipeline.builder()
            .inputs(List.of(topics.orderApproved(), topics.orderRejected()))
            .output(topics.orderProcessed())
            .keySerde(key)
            .inputSerde(serdes.of(OrderDecided.class))
            .outputSerde(serdes.of(OrderProcessed.class))
            .completion(() -> new CompletionProcessor(mapper, clock))
            .build());
  }
}
