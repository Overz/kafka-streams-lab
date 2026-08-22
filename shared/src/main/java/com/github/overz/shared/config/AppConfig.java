package com.github.overz.shared.config;

/**
 * O minimo que toda aplicacao Streams do lab precisa. Cada dominio estende com o que so
 * ele usa -- antifraud acrescenta as regras, orders acrescenta a validacao -- para que o
 * shared nao conheca regra de fraude e o orders nao carregue config que nao le.
 */
public interface AppConfig {

  KafkaConfig kafka();

  TelemetryConfig telemetry();
}
