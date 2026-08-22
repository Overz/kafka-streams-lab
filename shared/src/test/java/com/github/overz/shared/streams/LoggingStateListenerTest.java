package com.github.overz.shared.streams;

import org.apache.kafka.streams.KafkaStreams;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Observacao das transicoes de estado da aplicacao")
class LoggingStateListenerTest {

  @Test
  @DisplayName("Dado o start, Quando passa a RUNNING, Entao a transicao e notificada com os dois estados")
  void transicao() {
    final var vistas = new ArrayList<String>();
    final var listener = new LoggingStateListener("orders",
        (anterior, novo) -> vistas.add(anterior + "->" + novo));

    listener.onChange(KafkaStreams.State.RUNNING, KafkaStreams.State.REBALANCING);

    assertThat(vistas).containsExactly("REBALANCING->RUNNING");
  }

  @Test
  @DisplayName("Dado um erro, Quando o estado vira ERROR, Entao o observador tambem e notificado")
  void erro() {
    final var vistas = new ArrayList<String>();
    final var listener = new LoggingStateListener("orders",
        (anterior, novo) -> vistas.add(novo.toString()));

    listener.onChange(KafkaStreams.State.ERROR, KafkaStreams.State.RUNNING);

    assertThat(vistas).containsExactly("ERROR");
  }
}
