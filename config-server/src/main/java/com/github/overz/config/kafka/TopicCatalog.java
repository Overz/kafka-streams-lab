package com.github.overz.config.kafka;

import org.apache.kafka.clients.admin.NewTopic;

import java.util.HashSet;
import java.util.List;

/**
 * O conteudo de um arquivo de {@code resources/kafka}: os topicos de uma aplicacao e o
 * cluster onde eles vivem.
 *
 * <p>Um arquivo por aplicacao, e o nome do arquivo e o nome do dominio. O cluster fica
 * dentro do arquivo, e nao num mapa a parte, para que abrir orders.yaml responda "quais
 * topicos" e "onde" sem abrir mais nada.
 *
 * <p>Quem diz de qual arquivo veio e o {@link TopicCatalogLoader}: ele acrescenta o nome
 * as mensagens de erro daqui, entao este record nao precisa carregar essa informacao.
 *
 * @param bootstrapServers enderecos da rede docker -- quem provisiona roda em container
 */
public record TopicCatalog(
  String bootstrapServers,
  TopicDefaults defaults,
  List<TopicSpec> topics
) {

  public TopicCatalog {
    if (bootstrapServers == null || bootstrapServers.isBlank()) {
      throw new IllegalArgumentException("falta 'bootstrap-servers'");
    }

    defaults = defaults == null ? TopicDefaults.empty() : defaults;
    topics = topics == null ? List.of() : List.copyOf(topics);

    final var vistos = new HashSet<String>();
    final var repetidos = topics.stream()
      .map(TopicSpec::name)
      .filter(name -> !vistos.add(name))
      .distinct()
      .toList();

    if (!repetidos.isEmpty()) {
      throw new IllegalArgumentException("topico declarado mais de uma vez: " + repetidos);
    }
  }

  /** O que seria criado, com os defaults do arquivo ja resolvidos em cada topico. */
  public List<NewTopic> newTopics() {
    return topics.stream().map(spec -> spec.toNewTopic(defaults)).toList();
  }
}
