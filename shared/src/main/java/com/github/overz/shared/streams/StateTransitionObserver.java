package com.github.overz.shared.streams;

import org.apache.kafka.streams.KafkaStreams;

/**
 * Quem quer saber que a aplicacao mudou de estado. A telemetria implementa; o teste
 * implementa com uma lista.
 */
@FunctionalInterface
public interface StateTransitionObserver {

  void onTransition(KafkaStreams.State previous, KafkaStreams.State current);

  StateTransitionObserver NONE = (previous, current) -> {
  };
}
