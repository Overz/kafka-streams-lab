package com.github.overz.orders.stage;

import com.github.overz.orders.mapper.OrdersMapper;
import com.github.overz.shared.event.FraudAnalysisCompleted;
import com.github.overz.shared.event.OrderDecided;
import com.github.overz.shared.event.OrderForFraudAnalysis;
import com.github.overz.shared.telemetry.Telemetry;
import com.github.overz.shared.time.Clocks;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueJoiner;

// Junta o veredito do antifraude com o retrato do pedido analisado. Nenhum dos dois
// sozinho monta o OrderDecided: a analise nao traz valor nem moeda, e o retrato nao sabe
// da decisao.
@Slf4j
@RequiredArgsConstructor
public final class DecisionJoiner
  implements ValueJoiner<FraudAnalysisCompleted, OrderForFraudAnalysis, OrderDecided> {

  public static final String STAGE = "decision";

  private final OrdersMapper mapper;
  private final Telemetry telemetry;

  @Override
  public OrderDecided apply(final FraudAnalysisCompleted analysis, final OrderForFraudAnalysis order) {
    return telemetry.inStage(STAGE, analysis.orderId(), () -> {
      log.info("aplicando decisao do antifraude, orderId='{}', customerId='{}', decisao='{}', riskScore={}, razoes={}",
        analysis.orderId(), analysis.customerId(), analysis.decision(),
        analysis.riskScore(), analysis.reasons());

      final var decided = mapper.toDecided(analysis, order, Clocks.now());

      log.info("pedido decidido, orderId='{}', decisao='{}', totalAmount='{}', currency='{}'",
        decided.orderId(), decided.decision(), decided.totalAmount(), decided.currency());

      return decided;
    });
  }
}
