package com.github.overz.shared.streams;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.ContextualFixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;

import java.util.ArrayList;

// Ultima etapa antes de todo sink: tira os headers auxiliares para que nada com o prefixo
// interno chegue a um topico de saida.
//
// ContextualFixedKeyProcessor ja guarda o context; FixedKey porque limpar header nao pode
// reparticionar, e o tipo torna isso impossivel em vez de apenas improvavel.
@Slf4j
@RequiredArgsConstructor
public final class HeaderCleanupProcessor<K, V> extends ContextualFixedKeyProcessor<K, V, V> {

  private final String prefix;

  public HeaderCleanupProcessor() {
    this(EventHeaders.INTERNAL_PREFIX);
  }

  public static <K, V> FixedKeyProcessorSupplier<K, V, V> supplier() {
    return HeaderCleanupProcessor::new;
  }

  @Override
  public void process(final FixedKeyRecord<K, V> record) {
    final var removidos = new ArrayList<String>();

    for (final var header : record.headers()) {
      if (header.key().startsWith(prefix)) {
        removidos.add(header.key());
      }
    }

    // Fora do laco: remover durante a iteracao invalidaria o iterador.
    removidos.forEach(record.headers()::remove);

    if (!removidos.isEmpty()) {
      log.debug("removendo headers auxiliares antes do sink, chave='{}', removidos={}",
        record.key(), removidos);
    }

    context().forward(record);
  }
}
