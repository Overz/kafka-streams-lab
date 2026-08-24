package com.github.overz.orders.pipeline;

import com.github.overz.shared.event.Decision;
import com.github.overz.shared.event.FraudAnalysisCompleted;
import com.github.overz.shared.event.OrderDecided;
import com.github.overz.shared.event.OrderForFraudAnalysis;
import com.github.overz.shared.streams.HeaderCleanupProcessor;
import com.github.overz.shared.streams.Pipeline;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.state.KeyValueStore;

// Responsabilidade: aplicar ao pedido o veredito do antifraude.
//
//   <analise> + estado do pedido --+-- APPROVED -> orders.approved
//                                  \-- REJECTED -> orders.rejected
//
// O estado vem de orders.fraud-requested lido como tabela, e nao de orders.validated:
// aquele topico ja pertence a pipeline de pedido de analise, e o Kafka Streams so aceita
// um source por topico. Alem disso ele e o retrato exato do que o antifraude analisou.
//
// O nome do topico de analise vem da configuracao porque o MirrorMaker 2 o entrega
// prefixado com o alias do cluster de origem.
@Slf4j
@Builder
@RequiredArgsConstructor
public final class DecisionPipeline implements Pipeline {

  private final String analysisInput;
  private final String orderStateInput;
  private final String stateStore;
  private final String approvedOutput;
  private final String rejectedOutput;
  private final Serde<String> keySerde;
  private final Serde<FraudAnalysisCompleted> analysisSerde;
  private final Serde<OrderForFraudAnalysis> orderStateSerde;
  private final Serde<OrderDecided> decidedSerde;
  private final ValueJoiner<FraudAnalysisCompleted, OrderForFraudAnalysis, OrderDecided> decision;

  @Override
  public void build(final StreamsBuilder builder) {
    log.info("montando pipeline de decisao, analise='{}', estado='{}', saidas=['{}', '{}']",
      analysisInput, orderStateInput, approvedOutput, rejectedOutput);

    final var orderState = builder.table(orderStateInput,
      Consumed.with(keySerde, orderStateSerde).withName("order-state-source"),
      Materialized.<String, OrderForFraudAnalysis, KeyValueStore<Bytes, byte[]>>as(stateStore)
        .withKeySerde(keySerde)
        .withValueSerde(orderStateSerde));

    builder.stream(analysisInput, Consumed.with(keySerde, analysisSerde).withName("decision-source"))
      .join(orderState, decision,
        Joined.with(keySerde, analysisSerde, orderStateSerde).withName("decision-join"))
      .split(Named.as("decision-"))
      .branch(
        (key, decided) -> decided.decision() == Decision.APPROVED,
        Branched.withConsumer(approved -> sink(approved, approvedOutput, "approved"), "approved")
      )
      .defaultBranch(Branched.withConsumer(rejected -> sink(rejected, rejectedOutput, "rejected"), "rejected"));
  }

  private void sink(final KStream<String, OrderDecided> branch, final String topic, final String name) {
    branch
      .processValues(HeaderCleanupProcessor.supplier(), Named.as(name + "-cleanup"))
      .to(topic, Produced.with(keySerde, decidedSerde).withName(name + "-sink"));
  }
}
