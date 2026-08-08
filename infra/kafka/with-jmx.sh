#!/usr/bin/env bash
# Ativa o jmx_prometheus_javaagent APENAS no processo que este wrapper executa.
#
# Por que nao colocar KAFKA_OPTS direto no environment do container: toda ferramenta
# de linha de comando rodada via "docker exec" (kafka-topics.sh, kafka-metadata-quorum.sh,
# kafka-broker-api-versions.sh...) herda o environment e carrega o agent junto,
# que tenta bindar a mesma porta ja ocupada pelo servidor e mata o comando com
#
#   Failed to start Prometheus JMX Exporter
#   java.net.BindException: Address in use
#
# Passando por aqui, so o servidor recebe o agent; as ferramentas rodam limpas.
set -euo pipefail

JMX_EXPORTER_PORT="${JMX_EXPORTER_PORT:-7071}"

export KAFKA_OPTS="-javaagent:/opt/jmx/jmx_prometheus_javaagent.jar=${JMX_EXPORTER_PORT}:/opt/jmx/kafka.yml ${KAFKA_OPTS:-}"

exec "$@"
