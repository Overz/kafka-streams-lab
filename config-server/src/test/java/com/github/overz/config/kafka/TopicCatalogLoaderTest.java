package com.github.overz.config.kafka;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("Leitura dos arquivos de topico")
class TopicCatalogLoaderTest {

  private final TopicCatalogLoader loader = new TopicCatalogLoader();

  @Nested
  @DisplayName("Dados os arquivos reais de resources/kafka")
  class ArquivosReais {

    // Sem Spring: o objetivo e que um erro de digitacao no yaml quebre o build, nao que o
    // contexto suba. Rodar sem broker e sem contexto deixa esta guarda barata.
    @Test
    @DisplayName("Quando le o diretorio, Entao ha um catalogo por dominio")
    void umPorDominio() {
      final var catalogs = loader.load("classpath*:kafka/*.yaml");

      assertThat(catalogs).containsOnlyKeys("antifraud", "orders");
    }

    @Test
    @DisplayName("Quando le orders, Entao aponta para o cluster A e traz todos os topicos")
    void orders() {
      final var orders = loader.load("classpath*:kafka/*.yaml").get("orders");

      assertThat(orders.bootstrapServers()).contains("kafka-broker-a-1:9092");
      assertThat(orders.topics()).extracting(TopicSpec::name).containsExactly(
        "orders.created",
        "orders.validated",
        "orders.fraud-requested",
        "orders.approved",
        "orders.rejected",
        "orders.processed",
        "orders.dlq");
    }

    @Test
    @DisplayName("Quando le antifraud, Entao aponta para o cluster B")
    void antifraud() {
      final var antifraud = loader.load("classpath*:kafka/*.yaml").get("antifraud");

      assertThat(antifraud.bootstrapServers()).contains("kafka-broker-b-1:9092");
      assertThat(antifraud.topics()).extracting(TopicSpec::name)
        .containsExactly("antifraud.analysis-completed", "antifraud.dlq");
    }

    // O MirrorMaker 2 e quem cria os replicados, com o mesmo numero de particoes da
    // origem. Declarar um aqui criaria um topico vazio com o nome certo e o MM2 nao o
    // preencheria direito -- por isso a guarda.
    @Test
    @DisplayName("Quando le tudo, Entao nenhum topico replicado do MM2 esta declarado")
    void semTopicosReplicados() {
      final var todos = loader.load("classpath*:kafka/*.yaml").values().stream()
        .flatMap(catalog -> catalog.topics().stream())
        .map(TopicSpec::name)
        .toList();

      assertThat(todos).noneMatch(name -> name.startsWith("cluster-a.") || name.startsWith("cluster-b."));
    }
  }

  @Nested
  @DisplayName("Dada a resolucao dos defaults")
  class Defaults {

    @Test
    @DisplayName("Quando o topico nao declara nada, Entao herda particoes, rf e configs do arquivo")
    void herdaTudo() {
      final var orders = loader.load("classpath*:kafka/*.yaml").get("orders");

      final var created = orders.newTopics().stream()
        .filter(topic -> topic.name().equals("orders.created"))
        .findFirst()
        .orElseThrow();

      assertThat(created.numPartitions()).isEqualTo(3);
      assertThat(created.replicationFactor()).isEqualTo((short) 3);
      assertThat(created.configs()).containsEntry("min.insync.replicas", "2");
    }

    // O merge e por chave: a dlq declara retention e continua herdando min.insync.replicas.
    @Test
    @DisplayName("Quando o topico declara configs, Entao elas somam com as do default")
    void mesclaConfigs() {
      final var orders = loader.load("classpath*:kafka/*.yaml").get("orders");

      final var dlq = orders.newTopics().stream()
        .filter(topic -> topic.name().equals("orders.dlq"))
        .findFirst()
        .orElseThrow();

      assertThat(dlq.configs())
        .containsEntry("min.insync.replicas", "2")
        .containsEntry("retention.ms", "2592000000")
        .containsEntry("retention.bytes", "1073741824");
    }
  }

  @Nested
  @DisplayName("Dado um arquivo malformado")
  class Malformado {

    @Test
    @DisplayName("Quando ha chave desconhecida, Entao falha nomeando o arquivo")
    void chaveDesconhecida() {
      assertThatThrownBy(() -> loader.load("classpath*:kafka-invalido/chave-desconhecida.yaml"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("chave-desconhecida.yaml");
    }

    @Test
    @DisplayName("Quando falta bootstrap-servers, Entao falha dizendo o que falta")
    void semBootstrap() {
      assertThatThrownBy(() -> loader.load("classpath*:kafka-invalido/sem-bootstrap.yaml"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("sem-bootstrap.yaml")
        .hasMessageContaining("bootstrap-servers");
    }

    @Test
    @DisplayName("Quando o mesmo topico aparece duas vezes, Entao falha nomeando o topico")
    void duplicado() {
      assertThatThrownBy(() -> loader.load("classpath*:kafka-invalido/duplicado.yaml"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("duplicado.yaml")
        .hasMessageContaining("orders.created");
    }
  }
}
