package com.github.overz.shared.event;

import lombok.Builder;
import lombok.With;
import lombok.extern.jackson.Jacksonized;

import java.time.Instant;
import java.util.List;

@With
@Builder
@Jacksonized
public record FraudAnalysisCompleted(
    String orderId,
    String customerId,
    Decision decision,
    Integer riskScore,
    List<FraudReason> reasons,
    Instant analyzedAt
) {
}
