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
public record OrderCreated(
    String orderId,
    String customerId,
    List<OrderItem> items,
    BigDecimal totalAmount,
    String currency,
    Instant createdAt
) {
}
