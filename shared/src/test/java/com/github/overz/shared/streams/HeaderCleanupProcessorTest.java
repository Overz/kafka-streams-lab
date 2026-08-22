package com.github.overz.shared.streams;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Limpeza dos headers auxiliares antes do sink")
class HeaderCleanupProcessorTest {

  private static final String ENTRADA = "entrada";
  private static final String SAIDA = "saida";

  private TopologyTestDriver driver;
  private TestInputTopic<String, String> input;
  private TestOutputTopic<String, String> output;

  @BeforeEach
  void montarTopologia() {
    final var builder = new StreamsBuilder();

    builder.stream(ENTRADA, Consumed.with(Serdes.String(), Serdes.String()))
        .processValues(HeaderCleanupProcessor.<String, String>supplier(), Named.as("header-cleanup"))
        .to(SAIDA, Produced.with(Serdes.String(), Serdes.String()));

    final var props = new Properties();
    props.put("application.id", "header-cleanup-test");
    props.put("bootstrap.servers", "dummy:9092");

    driver = new TopologyTestDriver(builder.build(), props);
    input = driver.createInputTopic(ENTRADA, Serdes.String().serializer(), Serdes.String().serializer());
    output = driver.createOutputTopic(SAIDA, Serdes.String().deserializer(), Serdes.String().deserializer());
  }

  @AfterEach
  void fechar() {
    driver.close();
  }

  private static RecordHeader header(final String key, final String value) {
    return new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8));
  }

  private static String valorDe(final Headers headers, final String key) {
    final var found = headers.lastHeader(key);
    return found == null ? null : new String(found.value(), StandardCharsets.UTF_8);
  }

  @Test
  @DisplayName("Dado headers auxiliares, Quando o registro sai, Entao nenhum x-internal- sobrevive")
  void removeAuxiliares() {
    input.pipeInput(new TestRecord<>("ord-001", "payload", new RecordHeaders()
        .add(header(EventHeaders.INTERNAL_STAGE, "validation"))
        .add(header(EventHeaders.INTERNAL_RECEIVED_AT, "2026-08-07T18:30:00Z"))
        .add(header(EventHeaders.INTERNAL_ATTEMPT, "1"))));

    final var saida = output.readRecord();

    assertThat(saida.headers())
        .noneMatch(h -> h.key().startsWith(EventHeaders.INTERNAL_PREFIX));
  }

  @Test
  @DisplayName("Dado headers de entrada, Quando o registro sai, Entao eles sao preservados")
  void preservaHeadersDeEntrada() {
    input.pipeInput(new TestRecord<>("ord-001", "payload", new RecordHeaders()
        .add(header(EventHeaders.REQUEST_ID, "req-42"))
        .add(header(EventHeaders.TRACEPARENT, "00-abc-def-01"))
        .add(header(EventHeaders.INTERNAL_STAGE, "validation"))));

    final var saida = output.readRecord();

    assertThat(valorDe(saida.headers(), EventHeaders.REQUEST_ID)).isEqualTo("req-42");
    assertThat(valorDe(saida.headers(), EventHeaders.TRACEPARENT)).isEqualTo("00-abc-def-01");
    assertThat(saida.headers().toArray()).hasSize(2);
  }

  @Test
  @DisplayName("Dado headers de erro, Quando o registro vai para a DLQ, Entao o diagnostico e preservado")
  void preservaDiagnosticoDaDlq() {
    input.pipeInput(new TestRecord<>("ord-002", "payload", new RecordHeaders()
        .add(header(EventHeaders.ERROR_REASON, "TOTAL_AMOUNT_MISMATCH"))
        .add(header(EventHeaders.ERROR_STAGE, "validation"))
        .add(header(EventHeaders.ORIGINAL_TOPIC, "orders.created"))
        .add(header(EventHeaders.INTERNAL_STAGE, "validation"))));

    final var saida = output.readRecord();

    assertThat(valorDe(saida.headers(), EventHeaders.ERROR_REASON)).isEqualTo("TOTAL_AMOUNT_MISMATCH");
    assertThat(valorDe(saida.headers(), EventHeaders.ERROR_STAGE)).isEqualTo("validation");
    assertThat(valorDe(saida.headers(), EventHeaders.ORIGINAL_TOPIC)).isEqualTo("orders.created");
    assertThat(saida.headers()).noneMatch(h -> h.key().startsWith(EventHeaders.INTERNAL_PREFIX));
  }

  @Test
  @DisplayName("Dado um registro qualquer, Quando limpo, Entao chave e valor nao mudam")
  void preservaChaveEValor() {
    input.pipeInput(new TestRecord<>("ord-003", "payload-intacto", new RecordHeaders()
        .add(header(EventHeaders.INTERNAL_STAGE, "decision"))));

    final var saida = output.readRecord();

    assertThat(saida.key()).isEqualTo("ord-003");
    assertThat(saida.value()).isEqualTo("payload-intacto");
  }

  @Test
  @DisplayName("Dado um registro sem headers, Quando limpo, Entao passa sem erro")
  void semHeaders() {
    input.pipeInput("ord-004", "payload");

    final var saida = output.readRecord();

    assertThat(saida.key()).isEqualTo("ord-004");
    assertThat(saida.headers().toArray()).isEmpty();
  }
}
