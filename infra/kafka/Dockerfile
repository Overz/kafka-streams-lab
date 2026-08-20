# O Kafka so fala JMX; o OTel Collector so raspa Prometheus. O
# jmx_prometheus_javaagent faz a ponte, e precisa estar DENTRO do processo da JVM
# do Kafka -- por isso uma imagem propria em vez de um sidecar.
#
# O /metrics na 7071 nao sabe quem vai le-lo: trocar o backend (era
# grafana/otel-lgtm, hoje otelcol -> OpenObserve) nao muda nada aqui.
FROM apache/kafka:4.1.1

USER root

# 1.0.1 e a ultima publicada no Maven Central para este groupId (verificado em
# maven-metadata.xml). Nao existe 1.1+ -- o versionamento do projeto seguiu em
# outro artefato.
ARG JMX_AGENT_VERSION=1.0.1

ADD https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_AGENT_VERSION}/jmx_prometheus_javaagent-${JMX_AGENT_VERSION}.jar \
    /opt/jmx/jmx_prometheus_javaagent.jar

# Exemplo canonico do repo do jmx_exporter (tag 1.0.1, example_configs/kafka-2_0_0.yml)
# acrescido das regras de MirrorMaker 2 e dos clientes internos do Connect.
COPY jmx/kafka.yml /opt/jmx/kafka.yml
COPY with-jmx.sh /opt/jmx/with-jmx.sh

RUN chmod -R a+rX /opt/jmx && chmod a+x /opt/jmx/with-jmx.sh

USER appuser

# ENTRYPOINT herdado (/__cacert_entrypoint.sh) faz exec no CMD. Interpondo o
# wrapper aqui, o agent entra so no processo do servidor -- ver with-jmx.sh.
CMD ["/opt/jmx/with-jmx.sh", "/etc/kafka/docker/run"]
