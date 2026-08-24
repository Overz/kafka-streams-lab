import com.github.overz.orders.OrdersConfig;
import com.github.overz.orders.OrdersTopics;
import com.github.overz.orders.mapper.OrdersMapper;
import com.github.overz.orders.mapper.OrdersMapperImpl;
import com.github.overz.orders.pipeline.CompletionPipeline;
import com.github.overz.orders.pipeline.DecisionPipeline;
import com.github.overz.orders.pipeline.FraudRequestPipeline;
import com.github.overz.orders.pipeline.ValidationPipeline;
import com.github.overz.orders.rule.OrderValidationRules;
import com.github.overz.orders.stage.*;
import com.github.overz.shared.config.ConfigServerClient;
import com.github.overz.shared.event.Money;
import com.github.overz.shared.streams.Pipeline;
import com.github.overz.shared.streams.PipelineTopology;
import com.github.overz.shared.streams.StreamsRunner;
import com.github.overz.shared.telemetry.OpenTelemetryBootstrap;
import com.github.overz.shared.telemetry.StreamsMetricsBridge;
import com.github.overz.shared.telemetry.Telemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import org.slf4j.LoggerFactory;

import static com.github.overz.orders.OrdersSerdes.*;

// Ponto de entrada do dominio orders, como compact source file do Java 25 (JEP 512): sem
// declaracao de classe, sem static e sem String[] args.
//
// O preco e ficar no unnamed package -- compact source file nao aceita "package" -- e nao
// poder usar @Slf4j, que precisa de uma classe para anotar. Nao custa log aqui: o
// StreamsRunner ja registra applicationId e brokers, e cada pipeline registra seus topicos.
//
// E o unico lugar do modulo que conhece implementacoes concretas. As pipelines recebem
// ProcessorSupplier e ValueJoiner; quem decide o que ha por tras deles e este arquivo.
static final ConfigServerClient client = ConfigServerClient.shared();

  static final String application = client.application();
  static final OrdersConfig config = client.fetch(OrdersConfig.class);
  static final OrdersTopics topics = config.topics();

  static final OpenTelemetrySdk openTelemetry = OpenTelemetryBootstrap.start(application, config.telemetry());
  static final Telemetry telemetry = new Telemetry(application, openTelemetry.getTracer(application)
  );

  static final OrdersMapper mapper = new OrdersMapperImpl();
  static final OrderValidationRules rules = OrderValidationRules.standard(
    new Money(config.validation().moneyScale(), RoundingMode.HALF_UP)
  );

  void main() {
    final var log = LoggerFactory.getLogger(getClass());
    log.info("Starting Stream...");

    try (final var runner = StreamsRunner.of(
      application,
      new PipelineTopology(pipelines()),
      config.kafka()
    )) {
      // A ponte observa o ciclo de vida: as metricas de thread, task e store so existem depois
      // do RUNNING, e cada rebalanceamento cria outras tantas.
      runner.start(new StreamsMetricsBridge(openTelemetry.getMeter(application), runner::metrics));
      // Registrado antes do start: um Ctrl+C entre o start e o registro deixaria a
      // aplicacao morrer no meio de um commit e o proximo boot reprocessaria.
      Runtime.getRuntime().addShutdownHook(new Thread(runner::close, application + "-shutdown"));
      runner.await();
    } catch (Exception e) {
      log.error("Error starting Stream.", e);
      System.exit(1);
    }
  }

  List<Pipeline> pipelines() {
    return List.of(
      validation(),
      fraudRequest(),
      decision(),
      completion()
    );
  }

  Pipeline validation() {
    return ValidationPipeline.builder()
      .input(topics.orderCreated())
      .validOutput(topics.orderValidated())
      .deadLetterOutput(topics.deadLetter())
      .keySerde(STRING_SERDES)
      .inputSerde(ORDER_CREATED_JSON_SERDES)
      .validSerde(ORDER_VALIDATED_JSON_SERDES)
      .deadLetterSerde(DEAD_LETTER_JSON_SERDES)
      // O processor e o proprio supplier: cada task recebe a sua instancia via get(), e nao
      // um processor compartilhado entre threads.
      .validation(new ValidationProcessor(rules, mapper, ORDER_CREATED_JSON_SERDES.serializer(), telemetry))
      .deadLetter(new DeadLetterProcessor(config.deadLetter().maxPayloadLength()))
      .build();
  }

  Pipeline fraudRequest() {
    return FraudRequestPipeline.builder()
      .input(topics.orderValidated())
      .output(topics.fraudRequested())
      .keySerde(STRING_SERDES)
      .inputSerde(ORDER_VALIDATED_JSON_SERDES)
      .outputSerde(ORDER_FOR_FRAUD_ANALYSIS_JSON_SERDES)
      .projection(new FraudRequestProcessor(mapper, telemetry))
      .build();
  }

  Pipeline decision() {
    return DecisionPipeline.builder()
      .analysisInput(topics.analysisCompleted())
      .orderStateInput(topics.fraudRequested())
      .stateStore(config.stores().orderState())
      .approvedOutput(topics.orderApproved())
      .rejectedOutput(topics.orderRejected())
      .keySerde(STRING_SERDES)
      .analysisSerde(FRAUD_ANALYSIS_COMPLETED_JSON_SERDES)
      .orderStateSerde(ORDER_FOR_FRAUD_ANALYSIS_JSON_SERDES)
      .decidedSerde(ORDER_DECIDED_JSON_SERDES)
      .decision(new DecisionJoiner(mapper, telemetry))
      .build();
  }

  Pipeline completion() {
    return CompletionPipeline.builder()
      .inputs(List.of(topics.orderApproved(), topics.orderRejected()))
      .output(topics.orderProcessed())
      .keySerde(STRING_SERDES)
      .inputSerde(ORDER_DECIDED_JSON_SERDES)
      .outputSerde(ORDER_PROCESSED_JSON_SERDES)
      .completion(new CompletionProcessor(mapper))
      .build();
  }
