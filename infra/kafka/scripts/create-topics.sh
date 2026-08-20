#!/usr/bin/env bash
# Cria os topicos de cada cluster explicitamente.
#
# auto.create.topics.enable=false nos dois clusters e deliberado: topico criado
# automaticamente nasce com o default do broker e ninguem percebe se o RF saiu
# errado. Aqui o RF=3 e o min.insync.replicas=2 ficam visiveis e versionados.
#
# NAO cria os topicos replicados (cluster-a.orders.fraud-requested e
# cluster-b.antifraud.analysis-completed) -- quem cria e o MirrorMaker 2, com o
# mesmo numero de particoes da origem.
#
# Roda DENTRO de um container da imagem do lab (servico kafka-init-topics), nao
# no host: assim "docker compose up -d" sozinho ja deixa os topicos prontos, sem
# depender do Makefile. Por isso fala com os brokers por bootstrap-server na rede
# docker, e nao por "docker exec".
#
#   docker compose up -d kafka-init-topics     # recria, idempotente
set -euo pipefail

PARTITIONS="${PARTITIONS:-3}"
REPLICATION="${REPLICATION:-3}"
MIN_ISR="${MIN_ISR:-2}"

# Os tres enderecos de cada cluster, nao um: com so um, o broker daquele endereco
# fora do ar (exercicio 1) impede ate a descoberta do cluster.
BOOTSTRAP_A="${BOOTSTRAP_A:-kafka-broker-a-1:9092,kafka-broker-a-2:9092,kafka-broker-a-3:9092}"
BOOTSTRAP_B="${BOOTSTRAP_B:-kafka-broker-b-1:9092,kafka-broker-b-2:9092,kafka-broker-b-3:9092}"

KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/bin}"

# Topicos por cluster. Cada dominio e dono dos seus.
TOPICS_A=(
  "orders.created"
  "orders.validated"
  "orders.fraud-requested"
  "orders.processed"
)
TOPICS_B=(
  "antifraud.analysis-completed"
)

wait_for_cluster() {
  local bootstrap="$1"
  local attempt=0

  until "${KAFKA_BIN}/kafka-broker-api-versions.sh" \
      --bootstrap-server "${bootstrap}" >/dev/null 2>&1; do
    attempt=$((attempt + 1))
    if [ "${attempt}" -ge 30 ]; then
      echo "erro: ${bootstrap} nao respondeu depois de 30 tentativas" >&2
      return 1
    fi
    echo "  aguardando ${bootstrap}... (${attempt}/30)"
    sleep 5
  done
}

create_topics() {
  local bootstrap="$1"
  shift

  for topic in "$@"; do
    # --if-not-exists deixa o script idempotente: rodar de novo nao falha nem
    # altera topico existente.
    "${KAFKA_BIN}/kafka-topics.sh" \
      --bootstrap-server "${bootstrap}" \
      --create --if-not-exists \
      --topic "${topic}" \
      --partitions "${PARTITIONS}" \
      --replication-factor "${REPLICATION}" \
      --config "min.insync.replicas=${MIN_ISR}" >/dev/null
    echo "  ${topic}  particoes=${PARTITIONS} rf=${REPLICATION} min.isr=${MIN_ISR}"
  done
}

echo "cluster-a:"
wait_for_cluster "${BOOTSTRAP_A}"
create_topics "${BOOTSTRAP_A}" "${TOPICS_A[@]}"

echo "cluster-b:"
wait_for_cluster "${BOOTSTRAP_B}"
create_topics "${BOOTSTRAP_B}" "${TOPICS_B[@]}"
