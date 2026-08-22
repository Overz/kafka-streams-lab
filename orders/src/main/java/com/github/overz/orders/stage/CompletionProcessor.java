package com.github.overz.orders.stage;

import com.github.overz.orders.mapper.OrdersMapper;
import com.github.overz.shared.event.OrderDecided;
import com.github.overz.shared.event.OrderProcessed;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.ContextualFixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;

import java.time.Clock;

@Slf4j
@RequiredArgsConstructor
public final class CompletionProcessor
    extends ContextualFixedKeyProcessor<String, OrderDecided, OrderProcessed> {

  private final OrdersMapper mapper;
  private final Clock clock;

  @Override
  public void process(final FixedKeyRecord<String, OrderDecided> record) {
    final var processed = mapper.toProcessed(record.value(), clock.instant());

    log.info("finalizando ciclo do pedido, orderId='{}', customerId='{}', decisao='{}', riskScore={}, processedAt='{}'",
        processed.orderId(), processed.customerId(), processed.decision(),
        processed.riskScore(), processed.processedAt());

    context().forward(record.withValue(processed));
  }
}
