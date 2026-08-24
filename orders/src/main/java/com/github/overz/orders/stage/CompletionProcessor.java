package com.github.overz.orders.stage;

import com.github.overz.orders.mapper.OrdersMapper;
import com.github.overz.shared.event.OrderDecided;
import com.github.overz.shared.event.OrderProcessed;
import com.github.overz.shared.time.Clocks;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.ContextualFixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;

@Slf4j
@RequiredArgsConstructor
public final class CompletionProcessor implements FixedKeyProcessorSupplier<String, OrderDecided, OrderProcessed> {

  private final OrdersMapper mapper;

  @Override
  public FixedKeyProcessor<String, OrderDecided, OrderProcessed> get() {
    return new ContextualFixedKeyProcessor<>() {
      @Override
      public void process(FixedKeyRecord<String, OrderDecided> record) {
        final var processed = mapper.toProcessed(record.value(), Clocks.now());

        log.info("finalizando ciclo do pedido, orderId='{}', customerId='{}', decisao='{}', riskScore={}, processedAt='{}'",
          processed.orderId(), processed.customerId(), processed.decision(),
          processed.riskScore(), processed.processedAt());

        context().forward(record.withValue(processed));
      }
    };
  }
}
