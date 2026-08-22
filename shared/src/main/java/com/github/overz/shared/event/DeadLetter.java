package com.github.overz.shared.event;

import lombok.Builder;
import lombok.With;
import lombok.extern.jackson.Jacksonized;

import java.time.Instant;

@With
@Builder
@Jacksonized
public record DeadLetter(
    String key,
    String originalTopic,
    String stage,
    String reason,
    String detail,
    String payload,
    Instant failedAt
) {
}
