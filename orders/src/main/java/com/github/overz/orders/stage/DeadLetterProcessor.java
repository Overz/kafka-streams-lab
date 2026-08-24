package com.github.overz.orders.stage;

import com.github.overz.shared.event.DeadLetter;
import com.github.overz.shared.event.OrderValidated;
import com.github.overz.shared.streams.EventHeaders;
import com.github.overz.shared.streams.StageOutcome;
import com.github.overz.shared.time.Clocks;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.ContextualFixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;

import java.nio.charset.StandardCharsets;

// Monta o registro da dead letter queue e promove o contexto que viajava nos headers
// auxiliares para os headers de erro -- os unicos que sobrevivem a limpeza no sink.
@Slf4j
@RequiredArgsConstructor
public final class DeadLetterProcessor implements FixedKeyProcessorSupplier<String, StageOutcome<OrderValidated>, DeadLetter> {

  private final int maxPayloadLength;

  @Override
  public FixedKeyProcessor<String, StageOutcome<OrderValidated>, DeadLetter> get() {
    return new ContextualFixedKeyProcessor<>() {
      @Override
      public void process(FixedKeyRecord<String, StageOutcome<OrderValidated>> record) {
        if (!(record.value() instanceof StageOutcome.Failure<OrderValidated>(
          String reason, String stage, String detail, String payload
        ))) {
          log.error("registro sem falha chegou na dead letter queue, chave='{}'", record.key());
          return;
        }

        final var originalTopic = header(record, EventHeaders.INTERNAL_ORIGINAL_TOPIC);

        log.warn("enviando para a dead letter queue, chave='{}', motivo='{}', etapa='{}', topicoOrigem='{}'",
          record.key(), reason, stage, originalTopic);

        final var forwarded = record.withValue(DeadLetter.builder()
          .key(record.key())
          .originalTopic(originalTopic)
          .stage(stage)
          .reason(reason)
          .detail(detail)
          .payload(truncate(payload))
          .failedAt(Clocks.now())
          .build());

        stamp(forwarded, EventHeaders.ERROR_REASON, reason);
        stamp(forwarded, EventHeaders.ERROR_STAGE, stage);
        stamp(forwarded, EventHeaders.ERROR_DETAIL, detail);
        stamp(forwarded, EventHeaders.ORIGINAL_TOPIC, originalTopic);

        context().forward(forwarded);
      }

      private String truncate(final String payload) {
        return payload == null || payload.length() <= maxPayloadLength
          ? payload
          : payload.substring(0, maxPayloadLength);
      }

      private String header(final FixedKeyRecord<String, ?> record, final String key) {
        final var found = record.headers().lastHeader(key);
        return found == null ? null : new String(found.value(), StandardCharsets.UTF_8);
      }

      private void stamp(final FixedKeyRecord<String, ?> record, final String key, final String value) {
        if (value == null) {
          return;
        }
        record.headers().remove(key);
        record.headers().add(key, value.getBytes(StandardCharsets.UTF_8));
      }
    };
  }
}
