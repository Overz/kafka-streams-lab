package com.github.overz.orders.stage;

import com.github.overz.orders.mapper.OrdersMapper;
import com.github.overz.shared.event.OrderForFraudAnalysis;
import com.github.overz.shared.event.OrderValidated;
import com.github.overz.shared.telemetry.Telemetry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.ContextualFixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;

@Slf4j
@RequiredArgsConstructor
public final class FraudRequestProcessor implements FixedKeyProcessorSupplier<String, OrderValidated, OrderForFraudAnalysis> {

  public static final String STAGE = "fraud-request";

  private final OrdersMapper mapper;
  private final Telemetry telemetry;

  public FixedKeyProcessor<String, OrderValidated, OrderForFraudAnalysis> get() {
    return new ContextualFixedKeyProcessor<>() {
      @Override
      public void process(final FixedKeyRecord<String, OrderValidated> record) {
        final var validated = record.value();

        telemetry.inStage(STAGE, validated.orderId(), () -> {
          final var request = mapper.toFraudAnalysis(validated);

          log.info("enviando pedido para analise antifraude, orderId='{}', customerId='{}', totalAmount='{}', createdAt='{}'",
            request.orderId(), request.customerId(), request.totalAmount(), request.createdAt());

          context().forward(record.withValue(request));
          return null;
        });
      }
    };
  }
}
