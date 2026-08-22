package com.github.overz.orders;

import lombok.Builder;
import lombok.With;
import lombok.extern.jackson.Jacksonized;

/**
 * Nomes vindos do config-server. Nenhum deles pode ser constante em Java: o topico de
 * decisao chega prefixado pelo MirrorMaker 2 no lab (cluster-b.antifraud.analysis-completed)
 * e sem prefixo no teste de um broker so.
 */
@With
@Builder
@Jacksonized
public record OrdersTopics(
    String orderCreated,
    String analysisCompleted,
    String orderValidated,
    String fraudRequested,
    String orderApproved,
    String orderRejected,
    String orderProcessed,
    String deadLetter
) {
}
