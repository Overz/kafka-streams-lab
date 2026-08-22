package com.github.overz.orders.pipeline;

import com.github.overz.shared.event.DeadLetter;
import com.github.overz.shared.event.OrderCreated;
import com.github.overz.shared.event.OrderValidated;
import com.github.overz.shared.streams.HeaderCleanupProcessor;
import com.github.overz.shared.streams.Pipeline;
import com.github.overz.shared.streams.StageOutcome;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

// Responsabilidade: decidir se um pedido entra no fluxo.
//
//   orders.created --+-- valido ---> orders.validated
//                    \-- invalido -> orders.dlq
@Slf4j
@Builder
@RequiredArgsConstructor
public final class ValidationPipeline implements Pipeline {

  private final String input;
  private final String validOutput;
  private final String deadLetterOutput;
  private final Serde<String> keySerde;
  private final Serde<OrderCreated> inputSerde;
  private final Serde<OrderValidated> validSerde;
  private final Serde<DeadLetter> deadLetterSerde;
  private final ProcessorSupplier<String, OrderCreated, String, StageOutcome<OrderValidated>> validation;
  private final FixedKeyProcessorSupplier<String, StageOutcome<OrderValidated>, DeadLetter> deadLetter;

  @Override
  public void build(final StreamsBuilder builder) {
    log.info("montando pipeline de validacao, entrada='{}', saidas=['{}', '{}']",
        input, validOutput, deadLetterOutput);

    builder.stream(input, Consumed.with(keySerde, inputSerde).withName("validation-source"))
        .process(validation, Named.as("validation"))
        .split(Named.as("validation-"))
        .branch((key, outcome) -> outcome.isSuccess(),
            Branched.withConsumer(valid -> valid
                .mapValues(StageOutcome::value, Named.as("validation-unwrap"))
                .processValues(HeaderCleanupProcessor.supplier(), Named.as("validation-cleanup"))
                .to(validOutput, Produced.with(keySerde, validSerde).withName("validation-sink")), "valid"))
        // defaultBranch e nao um segundo predicado: um evento nao pode escapar dos dois e
        // sumir sem rastro.
        .defaultBranch(Branched.withConsumer(invalid -> invalid
            .processValues(deadLetter, Named.as("dead-letter"))
            .processValues(HeaderCleanupProcessor.supplier(), Named.as("dead-letter-cleanup"))
            .to(deadLetterOutput, Produced.with(keySerde, deadLetterSerde).withName("dead-letter-sink")), "invalid"));
  }
}
