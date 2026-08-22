package com.github.overz.shared.event;

import lombok.Builder;
import lombok.With;
import lombok.extern.jackson.Jacksonized;

import java.math.BigDecimal;

@With
@Builder
@Jacksonized
public record OrderItem(
    String productId,
    Integer quantity,
    BigDecimal unitPrice
) {
}
