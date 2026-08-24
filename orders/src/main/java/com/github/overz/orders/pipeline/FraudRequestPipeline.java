package com.github.overz.orders.pipeline;

import com.github.overz.shared.event.OrderForFraudAnalysis;
import com.github.overz.shared.event.OrderValidated;
import com.github.overz.shared.streams.HeaderCleanupProcessor;
import com.github.overz.shared.streams.Pipeline;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;

// Responsabilidade: pedir a analise antifraude.
//
//   orders.validated --> orders.fraud-requested
@Slf4j
@Builder
@RequiredArgsConstructor
public final class FraudRequestPipeline implements Pipeline {

  private final String input;
  private final String output;
  private final Serde<String> keySerde;
  private final Serde<OrderValidated> inputSerde;
  private final Serde<OrderForFraudAnalysis> outputSerde;
  private final FixedKeyProcessorSupplier<String, OrderValidated, OrderForFraudAnalysis> projection;

  @Override
  public void build(final StreamsBuilder builder) {
    log.info("montando pipeline de pedido de analise, entrada='{}', saida='{}'", input, output);

    builder.stream(input, Consumed.with(keySerde, inputSerde).withName("fraud-request-source"))
      .processValues(projection, Named.as("fraud-request"))
      .processValues(HeaderCleanupProcessor.supplier(), Named.as("fraud-request-cleanup"))
      .to(output, Produced.with(keySerde, outputSerde).withName("fraud-request-sink"));
  }
}
