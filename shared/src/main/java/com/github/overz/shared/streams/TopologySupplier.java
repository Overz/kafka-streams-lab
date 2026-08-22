package com.github.overz.shared.streams;

import org.apache.kafka.streams.Topology;

/**
 * O que cada dominio entrega ao runner. Existir como interface e o que permite ao
 * StreamsRunner nao conhecer orders nem antifraud.
 */
@FunctionalInterface
public interface TopologySupplier {

  Topology topology();
}
