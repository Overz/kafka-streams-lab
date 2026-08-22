package com.github.overz.shared.event;

import lombok.Builder;
import lombok.With;
import lombok.extern.jackson.Jacksonized;

import java.math.BigDecimal;
import java.time.Instant;

@With
@Builder
@Jacksonized
public record OrderValidated(
    String orderId,
    String customerId,
    BigDecimal totalAmount,
    String currency,
    // O documento nao lista createdAt neste evento (secao 4), mas o exige no proximo
    // (secao 5). Carregar aqui e o que permite a pipeline de analise ler direto do
    // topico, em vez de reconstruir a data a partir de um header auxiliar.
    Instant createdAt,
    Instant validatedAt
) {
}
