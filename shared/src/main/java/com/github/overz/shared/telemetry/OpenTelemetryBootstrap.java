package com.github.overz.shared.telemetry;

import com.github.overz.shared.config.TelemetryConfig;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.instrumentation.logback.appender.v1_0.OpenTelemetryAppender;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Inicializa o SDK do OpenTelemetry a partir da configuracao do lab, em vez de depender de
 * variaveis de ambiente ou de um -javaagent: o endpoint do collector e uma propriedade
 * como qualquer outra, e vem do config-server junto com o resto.
 */
@RequiredArgsConstructor
@Slf4j
public final class OpenTelemetryBootstrap {

  private final String application;
  private final TelemetryConfig config;

  public OpenTelemetrySdk start() {
    log.info("inicializando opentelemetry, aplicacao='{}', endpoint='{}', timeout='{}'",
        application, config.endpoint(), config.timeout());

    final var sdk = AutoConfiguredOpenTelemetrySdk.builder()
        // Sem isto o SDK tambem leria variaveis de ambiente OTEL_*, e a config efetiva
        // passaria a depender de onde o processo foi iniciado.
        .setResultAsGlobal()
        .addPropertiesSupplier(this::properties)
        .build()
        .getOpenTelemetrySdk();

    // O appender do Logback nasce sem SDK e vai bufferizando; so aqui ele passa a exportar.
    OpenTelemetryAppender.install(sdk);

    log.info("opentelemetry inicializado, aplicacao='{}'", application);
    return sdk;
  }

  private Map<String, String> properties() {
    final var props = new LinkedHashMap<String, String>();

    props.put("otel.service.name", application);
    props.put("otel.exporter.otlp.protocol", "grpc");
    props.put("otel.exporter.otlp.endpoint", config.endpoint());
    props.put("otel.exporter.otlp.timeout", String.valueOf(config.timeout().toMillis()));
    props.put("otel.traces.exporter", "otlp");
    props.put("otel.metrics.exporter", "otlp");
    props.put("otel.logs.exporter", "otlp");
    props.put("otel.resource.attributes", "service.name=%s,service.namespace=kafka-streams-lab"
        .formatted(application));

    // Capturas do appender do Logback. Ficam aqui, e nao no logback.xml: o SDK sobrescreve
    // o que o arquivo declara e avisa que ignorou.
    props.put("otel.instrumentation.logback-appender.experimental-log-attributes", "true");
    props.put("otel.instrumentation.logback-appender.experimental.capture-mdc-attributes", "*");
    props.put("otel.instrumentation.logback-appender.experimental.capture-key-value-pair-attributes", "true");

    return props;
  }

  /** Para teste e para quando o collector nao interessa: nenhum sinal sai do processo. */
  public static OpenTelemetry disabled() {
    return OpenTelemetry.noop();
  }
}
