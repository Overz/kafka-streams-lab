package com.github.overz.shared.streams;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;

@RequiredArgsConstructor
@Slf4j
public final class LoggingStateListener implements KafkaStreams.StateListener {

  private final String application;
  private final StateTransitionObserver observer;

  public LoggingStateListener(final String application) {
    this(application, StateTransitionObserver.NONE);
  }

  @Override
  public void onChange(final KafkaStreams.State current, final KafkaStreams.State previous) {
    if (current == KafkaStreams.State.ERROR) {
      log.error("aplicacao entrou em estado de erro, aplicacao='{}', anterior='{}', atual='{}'",
          application, previous, current);
    } else {
      log.info("transicao de estado, aplicacao='{}', anterior='{}', atual='{}'",
          application, previous, current);
    }

    observer.onTransition(previous, current);
  }
}
