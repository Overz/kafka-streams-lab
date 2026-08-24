package com.github.overz.orders;

import com.github.overz.shared.event.DeadLetter;
import com.github.overz.shared.event.FraudAnalysisCompleted;
import com.github.overz.shared.event.OrderCreated;
import com.github.overz.shared.event.OrderDecided;
import com.github.overz.shared.event.OrderForFraudAnalysis;
import com.github.overz.shared.event.OrderProcessed;
import com.github.overz.shared.event.OrderValidated;
import com.github.overz.shared.serde.JsonSerde;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import static com.github.overz.shared.config.ObjectMapperConfig.json;

/**
 * Todo tipo que o dominio orders coloca ou tira de um topico, declarado uma vez so. Nao ha
 * cache: sao oito objetos criados no boot. O que importa e a declaracao unica -- topico
 * ligado com o serde errado, ou sem serde nenhum, fica visivel aqui e nao no runtime.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class OrdersSerdes {

  public static final Serde<String> STRING_SERDES =
    Serdes.String();

  public static final Serde<OrderCreated> ORDER_CREATED_JSON_SERDES =
    new JsonSerde<>(json(), OrderCreated.class);

  public static final Serde<OrderValidated> ORDER_VALIDATED_JSON_SERDES =
    new JsonSerde<>(json(), OrderValidated.class);

  public static final Serde<OrderForFraudAnalysis> ORDER_FOR_FRAUD_ANALYSIS_JSON_SERDES =
    new JsonSerde<>(json(), OrderForFraudAnalysis.class);

  public static final Serde<FraudAnalysisCompleted> FRAUD_ANALYSIS_COMPLETED_JSON_SERDES =
    new JsonSerde<>(json(), FraudAnalysisCompleted.class);

  public static final Serde<OrderDecided> ORDER_DECIDED_JSON_SERDES =
    new JsonSerde<>(json(), OrderDecided.class);

  public static final Serde<OrderProcessed> ORDER_PROCESSED_JSON_SERDES =
    new JsonSerde<>(json(), OrderProcessed.class);

  public static final Serde<DeadLetter> DEAD_LETTER_JSON_SERDES =
    new JsonSerde<>(json(), DeadLetter.class);
}
