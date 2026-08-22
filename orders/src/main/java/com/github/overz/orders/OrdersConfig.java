package com.github.overz.orders;

import com.github.overz.shared.config.AppConfig;
import com.github.overz.shared.config.DeadLetterConfig;
import com.github.overz.shared.config.KafkaConfig;
import com.github.overz.shared.config.TelemetryConfig;
import lombok.Builder;
import lombok.With;
import lombok.extern.jackson.Jacksonized;

@With
@Builder
@Jacksonized
public record OrdersConfig(
    KafkaConfig kafka,
    TelemetryConfig telemetry,
    OrdersTopics topics,
    OrdersStores stores,
    DeadLetterConfig deadLetter,
    ValidationConfig validation
) implements AppConfig {

  @With
  @Builder
  @Jacksonized
  public record ValidationConfig(Integer moneyScale) {
  }
}
