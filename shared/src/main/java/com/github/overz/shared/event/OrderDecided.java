package com.github.overz.shared.event;

import lombok.Builder;
import lombok.With;
import lombok.extern.jackson.Jacksonized;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/**
 * Publicado em orders.approved e orders.rejected. Separado de OrderProcessed de proposito:
 * sao dois momentos do ciclo -- decidido e finalizado -- e fundi-los faria orders.approved
 * carregar um processedAt que ainda nao aconteceu.
 */
@With
@Builder
@Jacksonized
public record OrderDecided(
    String orderId,
    String customerId,
    Decision decision,
    Integer riskScore,
    List<FraudReason> reasons,
    BigDecimal totalAmount,
    String currency,
    Instant decidedAt
) {
}
