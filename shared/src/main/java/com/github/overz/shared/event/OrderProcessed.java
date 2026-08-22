package com.github.overz.shared.event;

import lombok.Builder;
import lombok.With;
import lombok.extern.jackson.Jacksonized;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

@With
@Builder
@Jacksonized
public record OrderProcessed(
    String orderId,
    String customerId,
    Decision decision,
    Integer riskScore,
    List<FraudReason> reasons,
    BigDecimal totalAmount,
    String currency,
    Instant processedAt
) {
}
