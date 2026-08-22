package com.github.overz.shared.streams;

/**
 * Nomes dos headers que circulam no lab. Sao constantes de protocolo entre os dois
 * dominios, nao comportamento: quem manipula header e o HeaderCleanupProcessor e as
 * proprias etapas.
 *
 * Tres familias:
 *   entrada    chegam do produtor e atravessam o pipeline inteiro ate a saida
 *   auxiliares prefixo x-internal-, existem so entre etapas e sao removidos no sink
 *   erro       so aparecem na DLQ, promovidos a partir dos auxiliares
 */
public final class EventHeaders {

  private EventHeaders() {
  }

  public static final String INTERNAL_PREFIX = "x-internal-";

  // Entrada -- vem da API e sobrevivem ate o topico final.
  public static final String REQUEST_ID = "x-request-id";
  public static final String TRACEPARENT = "traceparent";

  // Auxiliares -- morrem no sink.
  public static final String INTERNAL_STAGE = INTERNAL_PREFIX + "stage";
  public static final String INTERNAL_RECEIVED_AT = INTERNAL_PREFIX + "received-at";
  public static final String INTERNAL_ATTEMPT = INTERNAL_PREFIX + "attempt";
  public static final String INTERNAL_PARTITION_KEY = INTERNAL_PREFIX + "partition-key";
  public static final String INTERNAL_ORIGINAL_TOPIC = INTERNAL_PREFIX + "original-topic";

  // Erro -- so na DLQ.
  public static final String ERROR_REASON = "x-error-reason";
  public static final String ERROR_STAGE = "x-error-stage";
  public static final String ERROR_DETAIL = "x-error-detail";
  public static final String ORIGINAL_TOPIC = "x-original-topic";
}
