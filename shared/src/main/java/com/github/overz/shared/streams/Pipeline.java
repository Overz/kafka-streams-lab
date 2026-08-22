package com.github.overz.shared.streams;

import org.apache.kafka.streams.StreamsBuilder;

// Um trecho de fluxo com uma responsabilidade so: le de topico, faz uma coisa, escreve em
// topico. A topologia e a lista dessas unidades, e o encadeamento entre elas e o proprio
// Kafka -- nenhuma chama a outra.
//
// Um topico so pode ter um source na topologia inteira, entao duas pipelines nunca leem o
// mesmo topico. Onde isso pareceu necessario, foi sinal de responsabilidade mal dividida.
@FunctionalInterface
public interface Pipeline {

  void build(StreamsBuilder builder);
}
