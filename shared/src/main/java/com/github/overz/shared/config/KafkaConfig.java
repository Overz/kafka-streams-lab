package com.github.overz.shared.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.With;
import lombok.extern.jackson.Jacksonized;

import java.time.Duration;
import java.util.List;

@With
@Builder
@Jacksonized
public record KafkaConfig(
    @JsonDeserialize(using = CommaSeparatedListDeserializer.class)
    List<String> bootstrapServers,
    String applicationId,
    String clientId,
    String stateDir,
    Integer numStreamThreads,
    Duration commitInterval,
    String processingGuarantee,
    String deserializationExceptionHandler,
    Integer replicationFactor
) {
}
