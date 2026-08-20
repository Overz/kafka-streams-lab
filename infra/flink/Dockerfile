# Flink + conector Kafka do SQL.
#
# A imagem oficial nao traz conector nenhum: /opt/flink/lib so tem o core, os
# formatos (json, csv) e o planner. Sem o jar abaixo, "CREATE TABLE ... WITH
# ('connector'='kafka')" falha com "Could not find any factory for identifier
# 'kafka'".
#
# O sufixo do conector e a minor do Flink para a qual ele foi publicado: 5.0.0-2.2
# e a ultima no Maven Central, nao existe build para 2.3 ainda. Rodar 2.3.0 com o
# conector de 2.2 funciona -- verificado: factory resolvida, plano compilado e
# leitura de orders.created concluida. Ao subir a minor de novo, testar antes:
# quebra silenciosa aqui aparece como "Could not find any factory for identifier
# 'kafka'" ou NoSuchMethodError em tempo de execucao.
FROM apache/flink:2.3.0-java21

ARG KAFKA_CONNECTOR_VERSION=5.0.0-2.2

USER root
# ADD baixa direto, sem depender de curl/wget na imagem base. O --chmod e
# necessario porque o default (600, root) deixa o jar ilegivel para o usuario
# "flink", que e quem roda a JVM.
ADD --chmod=644 \
  https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar \
  /opt/flink/lib/flink-sql-connector-kafka.jar
USER flink
