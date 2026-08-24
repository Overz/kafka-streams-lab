package com.github.overz.shared.telemetry;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import lombok.RequiredArgsConstructor;

import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Um span por etapa do pipeline. A etapa recebe um Scope e nao o Span: assim ela nao
 * consegue esquecer de fechar nem inventar semantica de status propria.
 */
@RequiredArgsConstructor
public final class Telemetry {

  public static final AttributeKey<String> ORDER_ID = AttributeKey.stringKey("order.id");
  public static final AttributeKey<String> CUSTOMER_ID = AttributeKey.stringKey("customer.id");
  public static final AttributeKey<String> STAGE = AttributeKey.stringKey("pipeline.stage");
  public static final AttributeKey<String> REJECTION_REASON = AttributeKey.stringKey("rejection.reason");

  private final String application;
  private final Tracer tracer;

  /** O que a etapa pode dizer sobre o span sem conseguir manipula-lo. */
  @RequiredArgsConstructor
  public static final class Scope {

    private final Span span;
    private boolean rejected;

    public void rejected(final String reason) {
      this.rejected = true;
      span.setAttribute(REJECTION_REASON, reason);
      span.setStatus(StatusCode.ERROR, reason);
    }

    public void attribute(final AttributeKey<String> key, final String value) {
      span.setAttribute(key, value);
    }
  }

  public <T> T inStage(final String stage, final String orderId, final Supplier<T> body) {
    return inStage(stage, orderId, scope -> body.get());
  }

  public <T> T inStage(final String stage, final String orderId, final Function<Scope, T> body) {
    final var span = tracer.spanBuilder("%s.%s".formatted(application, stage))
        .setAttribute(STAGE, stage)
        .setAttribute(ORDER_ID, orderId == null ? "" : orderId)
        .startSpan();

    final var scope = new Scope(span);

    try (final var ignored = span.makeCurrent()) {
      final var result = body.apply(scope);

      // Nao basta chamar setStatus(OK) sempre: na especificacao do OpenTelemetry, OK e o
      // status mais forte e sobrescreve o ERROR que um rejected() acabou de gravar.
      if (!scope.rejected) {
        span.setStatus(StatusCode.OK);
      }
      return result;
    } catch (final RuntimeException e) {
      span.setStatus(StatusCode.ERROR, e.toString());
      span.recordException(e);
      throw e;
    } finally {
      span.end();
    }
  }
}
