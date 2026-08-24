package com.github.overz.orders;

import com.github.overz.orders.mapper.OrdersMapperImpl;
import com.github.overz.orders.pipeline.CompletionPipeline;
import com.github.overz.orders.pipeline.DecisionPipeline;
import com.github.overz.orders.pipeline.FraudRequestPipeline;
import com.github.overz.orders.pipeline.ValidationPipeline;
import com.github.overz.orders.rule.OrderValidationRules;
import com.github.overz.orders.stage.*;
import com.github.overz.shared.event.*;
import com.github.overz.shared.streams.EventHeaders;
import com.github.overz.shared.streams.Pipeline;
import com.github.overz.shared.streams.PipelineTopology;
import com.github.overz.shared.streams.StageOutcome;
import com.github.overz.shared.telemetry.Telemetry;
import io.opentelemetry.api.OpenTelemetry;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.*;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static com.github.overz.orders.OrdersSerdes.*;
import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Pipelines do dominio orders")
class OrdersPipelinesTest {

  private static final Instant AGORA = Instant.parse("2026-08-07T18:30:01Z");
  private static final Instant CRIADO_EM = Instant.parse("2026-08-07T18:30:00Z");

  private static final OrdersTopics TOPICOS = OrdersTopics.builder()
    .orderCreated("orders.created")
    .analysisCompleted("cluster-b.antifraud.analysis-completed")
    .orderValidated("orders.validated")
    .fraudRequested("orders.fraud-requested")
    .orderApproved("orders.approved")
    .orderRejected("orders.rejected")
    .orderProcessed("orders.processed")
    .deadLetter("orders.dlq")
    .build();

  private static final OrdersStores STORES = OrdersStores.builder()
    .orderState("orders-order-state-store")
    .build();

  private final Money money = Money.brl();
  private final Telemetry telemetry = new Telemetry("orders-test", OpenTelemetry.noop().getTracer("orders-test"));
  private final OrdersMapperImpl mapper = new OrdersMapperImpl();

  private TopologyTestDriver driver;

  private TestInputTopic<String, OrderCreated> criados;
  private TestInputTopic<String, OrderValidated> validadosEntrada;
  private TestInputTopic<String, FraudAnalysisCompleted> analises;
  private TestInputTopic<String, OrderDecided> decididosEntrada;

  private TestOutputTopic<String, OrderValidated> validados;
  private TestOutputTopic<String, OrderForFraudAnalysis> paraAnalise;
  private TestOutputTopic<String, OrderDecided> aprovados;
  private TestOutputTopic<String, OrderDecided> rejeitados;
  private TestOutputTopic<String, OrderProcessed> processados;
  private TestOutputTopic<String, DeadLetter> dlq;

  private ProcessorSupplier<String, OrderCreated, String, StageOutcome<OrderValidated>> validacaoPadrao() {
    return new ValidationProcessor(
      OrderValidationRules.standard(money), mapper,
      ORDER_CREATED_JSON_SERDES.serializer(), telemetry);
  }

  private List<Pipeline> pipelines(
    final ProcessorSupplier<String, OrderCreated, String, StageOutcome<OrderValidated>> validacao) {

    return List.of(
      ValidationPipeline.builder()
        .input(TOPICOS.orderCreated())
        .validOutput(TOPICOS.orderValidated())
        .deadLetterOutput(TOPICOS.deadLetter())
        .keySerde(STRING_SERDES)
        .inputSerde(ORDER_CREATED_JSON_SERDES)
        .validSerde(ORDER_VALIDATED_JSON_SERDES)
        .deadLetterSerde(DEAD_LETTER_JSON_SERDES)
        .validation(validacao)
        .deadLetter(new DeadLetterProcessor(4096))
        .build(),
      FraudRequestPipeline.builder()
        .input(TOPICOS.orderValidated())
        .output(TOPICOS.fraudRequested())
        .keySerde(STRING_SERDES)
        .inputSerde(ORDER_VALIDATED_JSON_SERDES)
        .outputSerde(ORDER_FOR_FRAUD_ANALYSIS_JSON_SERDES)
        .projection(new FraudRequestProcessor(mapper, telemetry))
        .build(),
      DecisionPipeline.builder()
        .analysisInput(TOPICOS.analysisCompleted())
        .orderStateInput(TOPICOS.fraudRequested())
        .stateStore(STORES.orderState())
        .approvedOutput(TOPICOS.orderApproved())
        .rejectedOutput(TOPICOS.orderRejected())
        .keySerde(STRING_SERDES)
        .analysisSerde(FRAUD_ANALYSIS_COMPLETED_JSON_SERDES)
        .orderStateSerde(ORDER_FOR_FRAUD_ANALYSIS_JSON_SERDES)
        .decidedSerde(ORDER_DECIDED_JSON_SERDES)
        .decision(new DecisionJoiner(mapper, telemetry))
        .build(),
      CompletionPipeline.builder()
        .inputs(List.of(TOPICOS.orderApproved(), TOPICOS.orderRejected()))
        .output(TOPICOS.orderProcessed())
        .keySerde(STRING_SERDES)
        .inputSerde(ORDER_DECIDED_JSON_SERDES)
        .outputSerde(ORDER_PROCESSED_JSON_SERDES)
        .completion(new CompletionProcessor(mapper))
        .build());
  }

  private void montar(final ProcessorSupplier<String, OrderCreated, String, StageOutcome<OrderValidated>> validacao) {
    final var props = new Properties();
    props.put("application.id", "orders-pipelines-test");
    props.put("bootstrap.servers", "dummy:9092");

    driver = new TopologyTestDriver(new PipelineTopology(pipelines(validacao)).topology(), props);

    criados = driver.createInputTopic(TOPICOS.orderCreated(),
      STRING_SERDES.serializer(), ORDER_CREATED_JSON_SERDES.serializer());
    validadosEntrada = driver.createInputTopic(TOPICOS.orderValidated(),
      STRING_SERDES.serializer(), ORDER_VALIDATED_JSON_SERDES.serializer());
    analises = driver.createInputTopic(TOPICOS.analysisCompleted(),
      STRING_SERDES.serializer(), FRAUD_ANALYSIS_COMPLETED_JSON_SERDES.serializer());
    decididosEntrada = driver.createInputTopic(TOPICOS.orderApproved(),
      STRING_SERDES.serializer(), ORDER_DECIDED_JSON_SERDES.serializer());

    validados = driver.createOutputTopic(TOPICOS.orderValidated(),
      STRING_SERDES.deserializer(), ORDER_VALIDATED_JSON_SERDES.deserializer());
    paraAnalise = driver.createOutputTopic(TOPICOS.fraudRequested(),
      STRING_SERDES.deserializer(), ORDER_FOR_FRAUD_ANALYSIS_JSON_SERDES.deserializer());
    aprovados = driver.createOutputTopic(TOPICOS.orderApproved(),
      STRING_SERDES.deserializer(), ORDER_DECIDED_JSON_SERDES.deserializer());
    rejeitados = driver.createOutputTopic(TOPICOS.orderRejected(),
      STRING_SERDES.deserializer(), ORDER_DECIDED_JSON_SERDES.deserializer());
    processados = driver.createOutputTopic(TOPICOS.orderProcessed(),
      STRING_SERDES.deserializer(), ORDER_PROCESSED_JSON_SERDES.deserializer());
    dlq = driver.createOutputTopic(TOPICOS.deadLetter(),
      STRING_SERDES.deserializer(), DEAD_LETTER_JSON_SERDES.deserializer());
  }

  @BeforeEach
  void montarPadrao() {
    montar(validacaoPadrao());
  }

  @AfterEach
  void fechar() {
    driver.close();
  }

  private static OrderCreated.OrderCreatedBuilder pedido() {
    return OrderCreated.builder()
      .orderId("ord-001")
      .customerId("cus-001")
      .items(List.of(new OrderItem("prod-001", 2, new BigDecimal("150.00"))))
      .totalAmount(new BigDecimal("300.00"))
      .currency("BRL")
      .createdAt(CRIADO_EM);
  }

  private static FraudAnalysisCompleted analise(final Decision decisao, final int score) {
    return FraudAnalysisCompleted.builder()
      .orderId("ord-001")
      .customerId("cus-001")
      .decision(decisao)
      .riskScore(score)
      .reasons(decisao == Decision.REJECTED ? List.of(FraudReason.HIGH_ORDER_AMOUNT) : List.of())
      .analyzedAt(Instant.parse("2026-08-07T18:30:02Z"))
      .build();
  }

  // Nenhum helper de encadeamento: quando um topico e sink de uma pipeline e source de
  // outra, o TopologyTestDriver entrega o registro de volta sozinho. E exatamente o que o
  // broker faz, e o que prova que as pipelines se ligam pelo topico e nao por codigo.

  @Nested
  @DisplayName("Dada a pipeline de validacao")
  class Validacao {

    @Test
    @DisplayName("Quando o pedido e valido, Entao sai em orders.validated com o createdAt original")
    void valido() {
      final var antes = Instant.now();

      criados.pipeInput("ord-001", pedido().build());

      assertThat(validados.readValue()).satisfies(v -> {
        assertThat(v.orderId()).isEqualTo("ord-001");
        assertThat(v.createdAt()).isEqualTo(CRIADO_EM);
        // Clocks.now() e o relogio do sistema: da para cravar a janela, nao o instante.
        assertThat(v.validatedAt()).isBetween(antes, Instant.now());
      });
      assertThat(dlq.isEmpty()).isTrue();
    }

    @Test
    @DisplayName("Quando o total diverge, Entao vai so para a dead letter queue")
    void totalDivergente() {
      criados.pipeInput("ord-002", pedido().orderId("ord-002")
        .totalAmount(new BigDecimal("999.00")).build());

      final var registro = dlq.readRecord();

      assertThat(registro.value().reason()).contains("TOTAL_AMOUNT_MISMATCH");
      assertThat(registro.value().detail()).contains("300.00").contains("999.00");
      assertThat(registro.value().originalTopic()).isEqualTo("orders.created");
      assertThat(registro.value().payload()).contains("ord-002");
      assertThat(validados.isEmpty()).isTrue();
    }

    @Test
    @DisplayName("Quando faltam varios campos, Entao a DLQ carrega todos os motivos")
    void variosMotivos() {
      criados.pipeInput("ord-003", pedido().orderId(null).currency(null).build());

      assertThat(dlq.readValue().reason())
        .contains("ORDER_ID_REQUIRED")
        .contains("CURRENCY_REQUIRED");
    }

    @Test
    @DisplayName("Quando vai para a DLQ, Entao os headers de erro estao presentes e limpos")
    void headersDeErro() {
      criados.pipeInput("ord-004", pedido().orderId("ord-004")
        .totalAmount(new BigDecimal("1.00")).build());

      final var headers = dlq.readRecord().headers();

      assertThat(valor(headers, EventHeaders.ERROR_REASON)).contains("TOTAL_AMOUNT_MISMATCH");
      assertThat(valor(headers, EventHeaders.ERROR_STAGE)).isEqualTo("validation");
      assertThat(valor(headers, EventHeaders.ORIGINAL_TOPIC)).isEqualTo("orders.created");
      assertThat(headers).noneMatch(h -> h.key().startsWith(EventHeaders.INTERNAL_PREFIX));
    }
  }

  @Nested
  @DisplayName("Dada a pipeline de pedido de analise")
  class PedidoDeAnalise {

    @Test
    @DisplayName("Quando le orders.validated, Entao publica o contrato do antifraude")
    void publicaContrato() {
      validadosEntrada.pipeInput("ord-001", OrderValidated.builder()
        .orderId("ord-001").customerId("cus-001")
        .totalAmount(new BigDecimal("300.00")).currency("BRL")
        .createdAt(CRIADO_EM).validatedAt(AGORA).build());

      assertThat(paraAnalise.readValue()).satisfies(r -> {
        assertThat(r.orderId()).isEqualTo("ord-001");
        assertThat(r.customerId()).isEqualTo("cus-001");
        assertThat(r.totalAmount()).isEqualByComparingTo("300.00");
        assertThat(r.currency()).isEqualTo("BRL");
        // createdAt do pedido, nao o instante da validacao.
        assertThat(r.createdAt()).isEqualTo(CRIADO_EM);
      });
    }

    @Test
    @DisplayName("Quando a validacao publica, Entao a pipeline seguinte consome do topico")
    void encadeamentoPorTopico() {
      criados.pipeInput("ord-001", pedido().build());

      assertThat(paraAnalise.readValue().orderId()).isEqualTo("ord-001");
    }
  }

  @Nested
  @DisplayName("Dada a pipeline de decisao")
  class DecisaoPipeline {

    @Test
    @DisplayName("Quando aprovado, Entao sai so em orders.approved com valor vindo do estado")
    void aprovado() {
      criados.pipeInput("ord-001", pedido().build());

      analises.pipeInput("ord-001", analise(Decision.APPROVED, 12));

      assertThat(aprovados.readValue()).satisfies(d -> {
        assertThat(d.decision()).isEqualTo(Decision.APPROVED);
        assertThat(d.riskScore()).isEqualTo(12);
        assertThat(d.totalAmount()).isEqualByComparingTo("300.00");
        assertThat(d.currency()).isEqualTo("BRL");
      });
      assertThat(rejeitados.isEmpty()).isTrue();
    }

    @Test
    @DisplayName("Quando rejeitado, Entao sai so em orders.rejected preservando as razoes")
    void rejeitado() {
      criados.pipeInput("ord-001", pedido().build());

      analises.pipeInput("ord-001", analise(Decision.REJECTED, 90));

      assertThat(rejeitados.readValue().reasons()).containsExactly(FraudReason.HIGH_ORDER_AMOUNT);
      assertThat(aprovados.isEmpty()).isTrue();
    }

    @Test
    @DisplayName("Quando o pedido nao esta no estado, Entao nada e publicado")
    void decisaoOrfa() {
      analises.pipeInput("ord-desconhecido",
        analise(Decision.APPROVED, 0).withOrderId("ord-desconhecido"));

      assertThat(aprovados.isEmpty()).isTrue();
      assertThat(rejeitados.isEmpty()).isTrue();
    }

    @Test
    @DisplayName("Quando o pedido e revalidado, Entao o estado usado e o mais recente")
    void estadoMaisRecente() {
      criados.pipeInput("ord-001", pedido().build());
      criados.pipeInput("ord-001", pedido().totalAmount(new BigDecimal("450.00"))
        .items(List.of(new OrderItem("prod-001", 3, new BigDecimal("150.00")))).build());

      analises.pipeInput("ord-001", analise(Decision.APPROVED, 0));

      assertThat(aprovados.readValue().totalAmount()).isEqualByComparingTo("450.00");
    }
  }

  @Nested
  @DisplayName("Dada a pipeline de finalizacao")
  class Finalizacao {

    @Test
    @DisplayName("Quando le orders.approved, Entao publica OrderProcessed")
    void aprovadoVira() {
      final var antes = Instant.now();

      decididosEntrada.pipeInput("ord-001", OrderDecided.builder()
        .orderId("ord-001").customerId("cus-001")
        .decision(Decision.APPROVED).riskScore(12).reasons(List.of())
        .totalAmount(new BigDecimal("300.00")).currency("BRL")
        .decidedAt(AGORA).build());

      assertThat(processados.readValue()).satisfies(p -> {
        assertThat(p.decision()).isEqualTo(Decision.APPROVED);
        assertThat(p.processedAt()).isBetween(antes, Instant.now());
      });
    }

    @Test
    @DisplayName("Quando o pedido percorre tudo, Entao ha exatamente um OrderProcessed")
    void umEventoFinal() {
      criados.pipeInput("ord-001", pedido().build());
      analises.pipeInput("ord-001", analise(Decision.APPROVED, 0));

      assertThat(processados.readRecordsToList()).hasSize(1);
    }
  }

  @Nested
  @DisplayName("Dado o contrato de headers")
  class HeadersDeSaida {

    @Test
    @DisplayName("Quando percorre as pipelines, Entao nenhuma saida carrega x-internal-")
    void saidasLimpas() {
      criados.pipeInput(new TestRecord<>("ord-001", pedido().build(), new RecordHeaders()
        .add(header(EventHeaders.REQUEST_ID, "req-42"))
        .add(header(EventHeaders.TRACEPARENT, "00-abc-def-01"))));

      analises.pipeInput("ord-001", analise(Decision.APPROVED, 0));

      assertThat(validados.readRecord().headers())
        .noneMatch(h -> h.key().startsWith(EventHeaders.INTERNAL_PREFIX));
      assertThat(paraAnalise.readRecord().headers())
        .noneMatch(h -> h.key().startsWith(EventHeaders.INTERNAL_PREFIX));
      assertThat(aprovados.readRecord().headers())
        .noneMatch(h -> h.key().startsWith(EventHeaders.INTERNAL_PREFIX));
      assertThat(processados.readRecord().headers())
        .noneMatch(h -> h.key().startsWith(EventHeaders.INTERNAL_PREFIX));
    }

    @Test
    @DisplayName("Quando o pedido traz rastreio, Entao ele sobrevive ate a saida")
    void rastreioSobrevive() {
      criados.pipeInput(new TestRecord<>("ord-001", pedido().build(), new RecordHeaders()
        .add(header(EventHeaders.REQUEST_ID, "req-42"))
        .add(header(EventHeaders.TRACEPARENT, "00-abc-def-01"))));

      final var validado = validados.readRecord();

      assertThat(valor(validado.headers(), EventHeaders.REQUEST_ID)).isEqualTo("req-42");
      assertThat(valor(validado.headers(), EventHeaders.TRACEPARENT)).isEqualTo("00-abc-def-01");
    }
  }

  @Nested
  @DisplayName("Dada uma etapa substituida")
  class EtapaSubstituida {

    @Test
    @DisplayName("Quando a validacao rejeita tudo, Entao as pipelines nao mudam e tudo vai para a DLQ")
    void pipelinesInalteradas() {
      driver.close();

      montar(() -> new ContextualProcessor<String, OrderCreated, String, StageOutcome<OrderValidated>>() {
        @Override
        public void process(final Record<String, OrderCreated> record) {
          context().forward(record.withValue(StageOutcome.failure(
            "SEMPRE_REJEITA", "validation", "etapa de teste")));
        }
      });

      criados.pipeInput("ord-001", pedido().build());

      assertThat(dlq.readValue().reason()).isEqualTo("SEMPRE_REJEITA");
      assertThat(validados.isEmpty()).isTrue();
    }
  }

  private static RecordHeader header(final String key, final String value) {
    return new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8));
  }

  private static String valor(final Headers headers, final String key) {
    return Optional.ofNullable(headers.lastHeader(key))
      .map(h -> new String(h.value(), StandardCharsets.UTF_8))
      .orElse(null);
  }
}
