package com.github.overz.orders.stage;

import com.github.overz.shared.event.DeadLetter;
import com.github.overz.shared.event.OrderValidated;
import com.github.overz.shared.streams.EventHeaders;
import com.github.overz.shared.streams.StageOutcome;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.ContextualFixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;

import java.nio.charset.StandardCharsets;
import java.time.Clock;

// Monta o registro da dead letter queue e promove o contexto que viajava nos headers
// auxiliares para os headers de erro -- os unicos que sobrevivem a limpeza no sink.
@Slf4j
@RequiredArgsConstructor
public final class DeadLetterProcessor
    extends ContextualFixedKeyProcessor<String, StageOutcome<OrderValidated>, DeadLetter> {

  private final Clock clock;
  private final int maxPayloadLength;

  @Override
  public void process(final FixedKeyRecord<String, StageOutcome<OrderValidated>> record) {
    if (!(record.value() instanceof StageOutcome.Failure<OrderValidated> failure)) {
      log.error("registro sem falha chegou na dead letter queue, chave='{}'", record.key());
      return;
    }

    final var originalTopic = header(record, EventHeaders.INTERNAL_ORIGINAL_TOPIC);

    log.warn("enviando para a dead letter queue, chave='{}', motivo='{}', etapa='{}', topicoOrigem='{}'",
        record.key(), failure.reason(), failure.stage(), originalTopic);

    final var forwarded = record.withValue(DeadLetter.builder()
        .key(record.key())
        .originalTopic(originalTopic)
        .stage(failure.stage())
        .reason(failure.reason())
        .detail(failure.detail())
        .payload(truncate(failure.payload()))
        .failedAt(clock.instant())
        .build());

    stamp(forwarded, EventHeaders.ERROR_REASON, failure.reason());
    stamp(forwarded, EventHeaders.ERROR_STAGE, failure.stage());
    stamp(forwarded, EventHeaders.ERROR_DETAIL, failure.detail());
    stamp(forwarded, EventHeaders.ORIGINAL_TOPIC, originalTopic);

    context().forward(forwarded);
  }

  // Payload gigante corrompido nao deve virar payload gigante corrompido em outro topico.
  private String truncate(final String payload) {
    return payload == null || payload.length() <= maxPayloadLength
        ? payload
        : payload.substring(0, maxPayloadLength);
  }

  private static String header(final FixedKeyRecord<String, ?> record, final String key) {
    final var found = record.headers().lastHeader(key);
    return found == null ? null : new String(found.value(), StandardCharsets.UTF_8);
  }

  private static void stamp(final FixedKeyRecord<String, ?> record, final String key, final String value) {
    if (value == null) {
      return;
    }
    record.headers().remove(key);
    record.headers().add(key, value.getBytes(StandardCharsets.UTF_8));
  }
}
