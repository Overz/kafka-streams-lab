# Lab Kafka multi-cluster. Ver docs/superpowers/specs/2026-08-07-kafka-multi-cluster-ha-design.md
#
# Quatro projetos compose separados de proposito: assim "make kill-cluster CLUSTER=b"
# derruba um cluster inteiro num comando, que e o exercicio 5.

COMPOSE ?= docker compose
IMAGE   ?= kafka-streams-lab/kafka:4.1.1

CLUSTER ?= a
N       ?= 1

.DEFAULT_GOAL := help

.PHONY: help build networks up down clean topics ps logs \
        kill-broker start-broker kill-controller start-controller \
        kill-cluster start-cluster quorum describe

help: ## Lista os alvos
	@grep -hE '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

build: ## Constroi a imagem do Kafka com o jmx_prometheus_javaagent
	docker build -t $(IMAGE) .

networks: ## Cria as redes cluster-a e cluster-b (idempotente)
	@docker network inspect cluster-a >/dev/null 2>&1 || docker network create cluster-a
	@docker network inspect cluster-b >/dev/null 2>&1 || docker network create cluster-b

up: networks build ## Sobe tudo na ordem: clusters, topicos, mirror, observabilidade
	@echo "==> cluster-a"
	$(COMPOSE) -f cluster-a.yaml up -d --wait
	@echo "==> cluster-b"
	$(COMPOSE) -f cluster-b.yaml up -d --wait
	@echo "==> topicos"
	./scripts/create-topics.sh
	@echo "==> mirror maker 2"
	$(COMPOSE) -f mirror.yaml up -d
	@echo "==> observabilidade"
	$(COMPOSE) -f observability.yaml up -d
	@echo ""
	@echo "  kafbat   http://localhost:8080"
	@echo "  grafana  http://localhost:3000"
	@echo "  cluster-a  localhost:19092,localhost:19093,localhost:19094"
	@echo "  cluster-b  localhost:29092,localhost:29093,localhost:29094"

down: ## Derruba tudo, preservando os volumes
	-$(COMPOSE) -f observability.yaml down
	-$(COMPOSE) -f mirror.yaml down
	-$(COMPOSE) -f cluster-b.yaml down
	-$(COMPOSE) -f cluster-a.yaml down

clean: down ## Derruba tudo E apaga volumes e redes
	-$(COMPOSE) -f cluster-a.yaml down -v
	-$(COMPOSE) -f cluster-b.yaml down -v
	-docker network rm cluster-a cluster-b

topics: ## (Re)cria os topicos -- idempotente
	./scripts/create-topics.sh

ps: ## Estado de todos os containers do lab
	@docker ps --filter "name=kafka-" --filter "name=kafbat" --filter "name=grafana-lgtm" \
		--format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

logs: ## Segue o log de um container: make logs N=kafka-mirror-1
	docker logs -f $(N)

# --- Exercicios de falha ---------------------------------------------------
# Todos usam "docker stop", nao "down": o container volta com "start-*" e os
# dados no volume permanecem, entao da para observar o ISR reconstruindo.

kill-broker: ## Derruba 1 broker: make kill-broker CLUSTER=a N=2
	docker stop kafka-broker-$(CLUSTER)-$(N)

start-broker: ## Restaura o broker: make start-broker CLUSTER=a N=2
	docker start kafka-broker-$(CLUSTER)-$(N)

kill-controller: ## Derruba 1 controller: make kill-controller CLUSTER=a N=2
	docker stop kafka-controller-$(CLUSTER)-$(N)

start-controller: ## Restaura o controller: make start-controller CLUSTER=a N=2
	docker start kafka-controller-$(CLUSTER)-$(N)

kill-cluster: ## Derruba um cluster inteiro: make kill-cluster CLUSTER=b
	$(COMPOSE) -f cluster-$(CLUSTER).yaml stop

start-cluster: ## Restaura o cluster: make start-cluster CLUSTER=b
	$(COMPOSE) -f cluster-$(CLUSTER).yaml start

# --- Inspecao --------------------------------------------------------------

quorum: ## Estado do quorum KRaft: make quorum CLUSTER=a
	docker exec kafka-broker-$(CLUSTER)-1 /opt/kafka/bin/kafka-metadata-quorum.sh \
		--bootstrap-server localhost:9092 describe --status

describe: ## ISR de todos os topicos: make describe CLUSTER=a
	docker exec kafka-broker-$(CLUSTER)-1 /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 --describe
