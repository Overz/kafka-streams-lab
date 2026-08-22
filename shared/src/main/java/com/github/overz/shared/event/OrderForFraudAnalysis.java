package com.github.overz.shared.event;

import lombok.Builder;
import lombok.With;
import lombok.extern.jackson.Jacksonized;

import java.math.BigDecimal;
import java.time.Instant;

@With
@Builder
@Jacksonized
public record OrderForFraudAnalysis(
    String orderId,
    String customerId,
    BigDecimal totalAmount,
    String currency,
    Instant createdAt
) {
}
