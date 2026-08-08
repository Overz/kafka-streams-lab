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
set -euo pipefail

PARTITIONS="${PARTITIONS:-3}"
REPLICATION="${REPLICATION:-3}"
MIN_ISR="${MIN_ISR:-2}"

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

wait_for_broker() {
  local container="$1"
  local attempt=0

  until docker exec "${container}" /opt/kafka/bin/kafka-broker-api-versions.sh \
      --bootstrap-server localhost:9092 >/dev/null 2>&1; do
    attempt=$((attempt + 1))
    if [ "${attempt}" -ge 30 ]; then
      echo "erro: ${container} nao respondeu depois de 30 tentativas" >&2
      return 1
    fi
    echo "  aguardando ${container}... (${attempt}/30)"
    sleep 5
  done
}

create_topics() {
  local container="$1"
  shift

  for topic in "$@"; do
    # --if-not-exists deixa o script idempotente: rodar de novo nao falha nem
    # altera topico existente.
    docker exec "${container}" /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 \
      --create --if-not-exists \
      --topic "${topic}" \
      --partitions "${PARTITIONS}" \
      --replication-factor "${REPLICATION}" \
      --config "min.insync.replicas=${MIN_ISR}"
    echo "  ${topic}  particoes=${PARTITIONS} rf=${REPLICATION} min.isr=${MIN_ISR}"
  done
}

echo "cluster-a:"
wait_for_broker "kafka-broker-a-1"
create_topics "kafka-broker-a-1" "${TOPICS_A[@]}"

echo "cluster-b:"
wait_for_broker "kafka-broker-b-1"
create_topics "kafka-broker-b-1" "${TOPICS_B[@]}"
