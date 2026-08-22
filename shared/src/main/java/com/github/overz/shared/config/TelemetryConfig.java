package com.github.overz.shared.config;

import lombok.Builder;
import lombok.With;
import lombok.extern.jackson.Jacksonized;

import java.time.Duration;

@With
@Builder
@Jacksonized
public record TelemetryConfig(
    String endpoint,
    Duration timeout
) {
}
