package com.github.overz.shared.config;

import lombok.Builder;
import lombok.With;
import lombok.extern.jackson.Jacksonized;

@With
@Builder
@Jacksonized
public record DeadLetterConfig(
    Integer maxPayloadLength
) {
}
