package com.github.overz.shared.streams;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

import java.util.List;

// A topologia de qualquer dominio: as pipelines, construidas no mesmo StreamsBuilder.
@Slf4j
@RequiredArgsConstructor
public final class PipelineTopology implements TopologySupplier {

  private final List<Pipeline> pipelines;

  @Override
  public Topology topology() {
    log.info("montando topologia, pipelines={}", pipelines.size());

    final var builder = new StreamsBuilder();
    pipelines.forEach(pipeline -> pipeline.build(builder));

    final var topology = builder.build();
    log.debug("topologia montada\n{}", topology.describe());

    return topology;
  }
}
