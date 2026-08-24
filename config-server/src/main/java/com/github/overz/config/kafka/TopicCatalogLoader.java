package com.github.overz.config.kafka;

import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.stereotype.Component;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.PropertyNamingStrategies;
import tools.jackson.dataformat.yaml.YAMLMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Le os arquivos de topico de um local no padrao de Resource do Spring.
 *
 * <p>O padrao default e {@code classpath*:kafka/*.yaml}, entao os arquivos viajam dentro
 * do jar. Apontar {@code lab.kafka.topics.location} para {@code file:/kafka/*.yaml} troca
 * para um diretorio montado, se um dia editar topico sem rebuild passar a importar.
 *
 * <p>Chave desconhecida no YAML derruba a leitura de proposito: um "retention-ms" escrito
 * onde deveria estar "configs.retention.ms" seria ignorado em silencio, e o topico nasceria
 * com a retencao errada sem ninguem perceber.
 */
@Component
public class TopicCatalogLoader {

  private static final ObjectMapper YAML = YAMLMapper.builder()
    // As chaves sao kebab-case; os componentes dos records sao camelCase. Mesma escolha
    // do ObjectMapperConfig do modulo shared para os .yaml do config-server.
    .propertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE)
    .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    .build();

  private final ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();

  /**
   * @return catalogos por nome de arquivo sem extensao, em ordem alfabetica -- o nome e o
   *     dominio dono dos topicos
   */
  public Map<String, TopicCatalog> load(final String location) {
    final Resource[] resources;

    try {
      resources = resolver.getResources(location);
    } catch (final IOException e) {
      throw new IllegalStateException("nao foi possivel listar '%s'".formatted(location), e);
    }

    final var catalogs = new LinkedHashMap<String, TopicCatalog>();

    Arrays.stream(resources)
      .sorted(Comparator.comparing(TopicCatalogLoader::domainOf))
      .forEach(resource -> catalogs.put(domainOf(resource), read(resource)));

    return catalogs;
  }

  private TopicCatalog read(final Resource resource) {
    try (final InputStream in = resource.getInputStream()) {
      return YAML.readValue(in, TopicCatalog.class);
    } catch (final IOException e) {
      throw new IllegalStateException("nao foi possivel ler '%s'".formatted(resource.getFilename()), e);
    } catch (final RuntimeException e) {
      // Acrescenta o arquivo a mensagem: sem isso o erro diz "falta 'bootstrap-servers'"
      // e nao diz de qual dos arquivos.
      throw new IllegalStateException(
        "'%s' invalido: %s".formatted(resource.getFilename(), rootMessage(e)), e);
    }
  }

  private static String domainOf(final Resource resource) {
    final var file = resource.getFilename();

    if (file == null) {
      return "desconhecido";
    }

    final var dot = file.lastIndexOf('.');
    return dot < 0 ? file : file.substring(0, dot);
  }

  private static String rootMessage(final Throwable e) {
    var cause = e;
    while (cause.getCause() != null) {
      cause = cause.getCause();
    }
    return cause.getMessage();
  }
}
