package com.github.overz.orders.stage;

import com.github.overz.orders.mapper.OrdersMapper;
import com.github.overz.orders.rule.OrderValidationRule;
import com.github.overz.orders.rule.OrderValidationRules;
import com.github.overz.shared.event.OrderCreated;
import com.github.overz.shared.event.OrderValidated;
import com.github.overz.shared.streams.EventHeaders;
import com.github.overz.shared.streams.StageOutcome;
import com.github.overz.shared.telemetry.Telemetry;
import com.github.overz.shared.time.Clocks;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.processor.api.*;
import org.apache.kafka.streams.processor.api.Record;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
public final class ValidationProcessor implements ProcessorSupplier<String, OrderCreated, String, StageOutcome<OrderValidated>> {

  public static final String STAGE = "validation";

  private final OrderValidationRules rules;
  private final OrdersMapper mapper;
  private final Serializer<OrderCreated> serializer;
  private final Telemetry telemetry;

  @Override
  public Processor<String, OrderCreated, String, StageOutcome<OrderValidated>> get() {
    return new ContextualProcessor<>() {
      @Override
      public void process(Record<String, OrderCreated> record) {
        final var order = record.value();

        if (order == null) {
          log.warn("descartando tombstone na validacao, chave='{}'", record.key());
          return;
        }

        log.info("iniciando validacao do pedido, orderId='{}', customerId='{}', totalAmount='{}', itens={}",
          order.orderId(), order.customerId(), order.totalAmount(),
          order.items() == null ? 0 : order.items().size());

        telemetry.inStage(STAGE, order.orderId(), scope -> {
          final var violations = rules.validate(order);

          if (violations.isEmpty()) {
            scope.attribute(Telemetry.CUSTOMER_ID, String.valueOf(order.customerId()));
            forwardValidated(record, order);
          } else {
            scope.rejected(reasons(violations));
            forwardFailure(record, order, violations);
          }

          return null;
        });
      }

      private void forwardValidated(final Record<String, OrderCreated> record, final OrderCreated order) {
        final var now = Clocks.now();
        final var validated = mapper.toValidated(order, now);

        log.info("pedido validado, orderId='{}', customerId='{}', validatedAt='{}'",
          validated.orderId(), validated.customerId(), validated.validatedAt());

        final var forwarded = record.withValue(StageOutcome.<OrderValidated>success(validated));

        stamp(forwarded, EventHeaders.INTERNAL_STAGE, STAGE);
        stamp(forwarded, EventHeaders.INTERNAL_RECEIVED_AT, now.toString());

        context().forward(forwarded);
      }

      private void forwardFailure(final Record<String, OrderCreated> record,
                                  final OrderCreated order,
                                  final List<OrderValidationRule.Violation> violations) {
        final var reasons = reasons(violations);
        final var detail = details(violations);
        final var sourceTopic = sourceTopic();

        log.warn("pedido rejeitado na validacao, orderId='{}', customerId='{}', motivos='{}', detalhe='{}'",
          order.orderId(), order.customerId(), reasons, detail);

        final var payload = new String(serializer.serialize(sourceTopic, order), StandardCharsets.UTF_8);
        final var failure = new StageOutcome.Failure<OrderValidated>(reasons, STAGE, detail, payload);
        final var forwarded = record.withValue((StageOutcome<OrderValidated>) failure);

        stamp(forwarded, EventHeaders.INTERNAL_STAGE, STAGE);
        stamp(forwarded, EventHeaders.INTERNAL_ORIGINAL_TOPIC, sourceTopic);

        context().forward(forwarded);
      }

      // Na Processor API o topico de origem vem do recordMetadata, ausente em punctuator.
      private String sourceTopic() {
        return context().recordMetadata().map(RecordMetadata::topic).orElse("");
      }

      private String reasons(final List<OrderValidationRule.Violation> violations) {
        return violations.stream()
          .map(v -> v.reason().name())
          .collect(Collectors.joining(","));
      }

      private String details(final List<OrderValidationRule.Violation> violations) {
        final var joined = violations.stream()
          .map(OrderValidationRule.Violation::detail)
          .filter(Objects::nonNull)
          .collect(Collectors.joining("; "));

        return joined.isEmpty() ? null : joined;
      }

      private void stamp(final Record<String, ?> record, final String key, final String value) {
        record.headers().remove(key);
        record.headers().add(key, value.getBytes(StandardCharsets.UTF_8));
      }
    };
  }
}
