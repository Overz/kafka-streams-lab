# Lab Kafka multi-cluster.
#   desenho:  docs/superpowers/specs/2026-08-07-kafka-multi-cluster-ha-design.md
#   uso:      infra/kafka/README.md
#
# O docker-compose.yaml da raiz inclui os quatro arquivos de infra/kafka/, entao os
# comandos "docker compose" abaixo nao precisam de -f.

COMPOSE ?= docker compose
IMAGE   ?= kafka-streams-lab/kafka:4.1.1

CLUSTER ?= a
N       ?= 1

# Servicos de um cluster inteiro, para os alvos kill-cluster/start-cluster.
CLUSTER_SERVICES = kafka-controller-$(CLUSTER)-1 kafka-controller-$(CLUSTER)-2 kafka-controller-$(CLUSTER)-3 \
                   kafka-broker-$(CLUSTER)-1 kafka-broker-$(CLUSTER)-2 kafka-broker-$(CLUSTER)-3

.DEFAULT_GOAL := help

.PHONY: help build up down clean topics ps logs \
        kill-broker start-broker kill-controller start-controller \
        kill-cluster start-cluster kill-mirror start-mirror \
        observability-up observability-down quorum describe

help: ## Lista os alvos
	@grep -hE '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

build: ## Constroi a imagem do Kafka com o jmx_prometheus_javaagent
	docker build -t $(IMAGE) infra/kafka

up: build ## Sobe tudo e cria os topicos
	$(COMPOSE) up -d --wait
	@echo "==> topicos"
	./infra/kafka/scripts/create-topics.sh
	@echo ""
	@echo "  kafbat     http://localhost:8080"
	@echo "  grafana    http://localhost:3000"
	@echo "  cluster-a  localhost:19092,localhost:19093,localhost:19094"
	@echo "  cluster-b  localhost:29092,localhost:29093,localhost:29094"

down: ## Derruba tudo, preservando os volumes
	$(COMPOSE) down

clean: ## Derruba tudo E apaga volumes e redes
	$(COMPOSE) down -v

topics: ## (Re)cria os topicos -- idempotente
	./infra/kafka/scripts/create-topics.sh

ps: ## Estado dos containers do lab
	@$(COMPOSE) ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"

logs: ## Segue o log de um servico: make logs N=kafka-mirror-1
	$(COMPOSE) logs -f $(N)

# --- Exercicios de falha ---------------------------------------------------
# Tudo com "stop", nunca "down": os volumes ficam, entao da para observar o ISR
# sendo reconstruido e o MM2 drenando o lag acumulado ao restaurar.

kill-broker: ## Derruba 1 broker: make kill-broker CLUSTER=a N=2
	$(COMPOSE) stop kafka-broker-$(CLUSTER)-$(N)

start-broker: ## Restaura o broker: make start-broker CLUSTER=a N=2
	$(COMPOSE) start kafka-broker-$(CLUSTER)-$(N)

kill-controller: ## Derruba 1 controller: make kill-controller CLUSTER=a N=2
	$(COMPOSE) stop kafka-controller-$(CLUSTER)-$(N)

start-controller: ## Restaura o controller: make start-controller CLUSTER=a N=2
	$(COMPOSE) start kafka-controller-$(CLUSTER)-$(N)

kill-cluster: ## Derruba um cluster inteiro: make kill-cluster CLUSTER=b
	$(COMPOSE) stop $(CLUSTER_SERVICES)

start-cluster: ## Restaura o cluster: make start-cluster CLUSTER=b
	$(COMPOSE) start $(CLUSTER_SERVICES)

kill-mirror: ## Derruba 1 instancia do MM2: make kill-mirror N=1
	$(COMPOSE) stop kafka-mirror-$(N)

start-mirror: ## Restaura a instancia do MM2: make start-mirror N=1
	$(COMPOSE) start kafka-mirror-$(N)

observability-up: ## Sobe kafbat e grafana-lgtm
	$(COMPOSE) start kafbat grafana-lgtm

observability-down: ## Para kafbat e grafana-lgtm para liberar RAM
	$(COMPOSE) stop kafbat grafana-lgtm

# --- Inspecao --------------------------------------------------------------

quorum: ## Estado do quorum KRaft: make quorum CLUSTER=a
	docker exec kafka-broker-$(CLUSTER)-1 /opt/kafka/bin/kafka-metadata-quorum.sh \
		--bootstrap-server localhost:9092 describe --status

describe: ## ISR de todos os topicos: make describe CLUSTER=a
	docker exec kafka-broker-$(CLUSTER)-1 /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 --describe
