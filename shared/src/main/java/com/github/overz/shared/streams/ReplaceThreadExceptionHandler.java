package com.github.overz.shared.streams;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

/**
 * Substitui a thread da task em vez de derrubar a aplicacao.
 *
 * O caso legitimo de excecao aqui e falha transitoria de infraestrutura -- broker
 * reiniciando, rebalanceamento. Evento invalido nao chega neste ponto: as etapas devolvem
 * StageOutcome.Failure e a topologia o roteia para a DLQ.
 */
@RequiredArgsConstructor
@Slf4j
public final class ReplaceThreadExceptionHandler implements StreamsUncaughtExceptionHandler {

  private final String application;

  @Override
  public StreamThreadExceptionResponse handle(final Throwable exception) {
    log.error("excecao nao capturada numa task, aplicacao='{}', decisao='REPLACE_THREAD', causa='{}'",
        application, exception.toString(), exception);

    return StreamThreadExceptionResponse.REPLACE_THREAD;
  }
}
