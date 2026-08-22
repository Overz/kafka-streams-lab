package com.github.overz.orders.mapper;

import com.github.overz.shared.event.FraudAnalysisCompleted;
import com.github.overz.shared.event.OrderCreated;
import com.github.overz.shared.event.OrderDecided;
import com.github.overz.shared.event.OrderForFraudAnalysis;
import com.github.overz.shared.event.OrderProcessed;
import com.github.overz.shared.event.OrderValidated;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

import java.time.Instant;

/**
 * Toda conversao entre eventos passa por aqui. Escrever os builders a mao deixaria um campo
 * esquecido virar null num topico sem que nada avisasse; com unmappedTargetPolicy = ERROR,
 * o mesmo esquecimento quebra o build.
 */
@Mapper(unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface OrdersMapper {

  @Mapping(target = "validatedAt", source = "validatedAt")
  OrderValidated toValidated(OrderCreated order, Instant validatedAt);

  OrderForFraudAnalysis toFraudAnalysis(OrderValidated validated);

  // A decisao traz o veredito; o estado do pedido traz valor e moeda. Nenhum dos dois
  // sozinho monta o OrderDecided -- e o motivo de o join existir.
  @Mapping(target = "orderId", source = "analysis.orderId")
  @Mapping(target = "customerId", source = "analysis.customerId")
  @Mapping(target = "decision", source = "analysis.decision")
  @Mapping(target = "riskScore", source = "analysis.riskScore")
  @Mapping(target = "reasons", source = "analysis.reasons")
  @Mapping(target = "totalAmount", source = "order.totalAmount")
  @Mapping(target = "currency", source = "order.currency")
  @Mapping(target = "decidedAt", source = "decidedAt")
  OrderDecided toDecided(FraudAnalysisCompleted analysis, OrderForFraudAnalysis order, Instant decidedAt);

  @Mapping(target = "processedAt", source = "processedAt")
  OrderProcessed toProcessed(OrderDecided decided, Instant processedAt);
}
