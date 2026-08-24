package com.github.overz.orders.pipeline;

import com.github.overz.shared.event.OrderDecided;
import com.github.overz.shared.event.OrderProcessed;
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

import java.util.List;

// Responsabilidade: fechar o ciclo do pedido.
//
//   orders.approved + orders.rejected --> orders.processed
//
// Le os dois desfechos num source so: orders.processed marca o fim do ciclo, aprovado ou nao.
@Slf4j
@Builder
@RequiredArgsConstructor
public final class CompletionPipeline implements Pipeline {

  private final List<String> inputs;
  private final String output;
  private final Serde<String> keySerde;
  private final Serde<OrderDecided> inputSerde;
  private final Serde<OrderProcessed> outputSerde;
  private final FixedKeyProcessorSupplier<String, OrderDecided, OrderProcessed> completion;

  @Override
  public void build(final StreamsBuilder builder) {
    log.info("montando pipeline de finalizacao, entradas={}, saida='{}'", inputs, output);

    builder.stream(inputs, Consumed.with(keySerde, inputSerde).withName("completion-source"))
      .processValues(completion, Named.as("completion"))
      .processValues(HeaderCleanupProcessor.supplier(), Named.as("completion-cleanup"))
      .to(output, Produced.with(keySerde, outputSerde).withName("completion-sink"));
  }
}
