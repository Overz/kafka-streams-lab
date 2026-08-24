# Lab Kafka multi-cluster.
#   desenho:  docs/superpowers/specs/2026-08-07-kafka-multi-cluster-ha-design.md
#   uso:      infra/README.md
#
# O docker-compose.yaml da raiz inclui um arquivo por dominio em infra/, entao os
# comandos "docker compose" abaixo nao precisam de -f.

COMPOSE ?= docker compose
IMAGE   ?= kafka-streams-lab/kafka:4.1.1

CLUSTER ?= a
N       ?= 1

# Alvo config. Variaveis proprias, e nao N: N e o ordinal de broker/controller/mirror e
# nunca fica vazio, entao um fallback com $(or $(N),...) jamais dispararia.
APP     ?= orders
PROFILE ?=

# Servico do compose para o alvo build. Vazio constroi todas as imagens.
SERVICE ?=

# Servicos de um cluster inteiro, para os alvos kill-cluster/start-cluster.
CLUSTER_SERVICES = kafka-controller-$(CLUSTER)-1 kafka-controller-$(CLUSTER)-2 kafka-controller-$(CLUSTER)-3 \
                   kafka-broker-$(CLUSTER)-1 kafka-broker-$(CLUSTER)-2 kafka-broker-$(CLUSTER)-3

.DEFAULT_GOAL := help

.PHONY: help build build-api build-config-server up down clean topics ps logs \
        apps-build orders antifraud apps-up e2e config \
        kill-broker start-broker kill-controller start-controller \
        kill-cluster start-cluster kill-mirror start-mirror \
        observability-up observability-down openobserve-restart quorum describe \
        flink-sql flink-example flink-up flink-down

help: ## Lista os alvos
	@grep -hE '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# As imagens que o lab constroi: Kafka com javaagent, Flink com conector Kafka, o
# config-server e a API. O resto vem pronto do registry.
build: ## Constroi todas as imagens do lab; uma so: make build SERVICE=api
	$(COMPOSE) build $(SERVICE)

build-api: ## Constroi so a imagem da API (Go)
	$(COMPOSE) build api

build-config-server: ## Constroi so a imagem do config-server (Java)
	$(COMPOSE) build config-server

up: ## Sobe tudo (constroi as imagens e cria os topicos no caminho)
	$(COMPOSE) up -d --wait
	@echo ""
	@echo "  kafbat      http://localhost:8080   (login: admin@admin / admin)"
	@echo "  openobserve http://localhost:5080   (SSO pelo Dex, ou admin@admin.com / Admin@123)"
	@echo "  flink       http://localhost:8081"
	@echo "  dex         http://localhost:5556/dex"
	@echo "  cluster-a  localhost:19092,localhost:19093,localhost:19094"
	@echo "  cluster-b  localhost:29092,localhost:29093,localhost:29094"

down: ## Derruba tudo, preservando os volumes
	$(COMPOSE) down

# tmp/ vai junto: e onde vive todo o descartavel do lado do host -- o state.dir das apps
# Streams e a saida de build da API. Apagar so os volumes deixaria as apps subindo com um
# checkpoint que aponta para offsets de um cluster que nao existe mais.
clean: ## Derruba tudo E apaga volumes, redes e a tmp/ local
	$(COMPOSE) down -v
	rm -rf tmp/

topics: ## (Re)cria os topicos -- idempotente; quem cria e o config-server, no boot
	$(COMPOSE) restart config-server
	@echo "  topicos reprovisionados; veja o log: docker compose logs config-server | grep -i topico"

ps: ## Estado dos containers do lab
	@$(COMPOSE) ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"

logs: ## Segue o log de um servico: make logs N=kafka-mirror-1
	$(COMPOSE) logs -f $(N)

# --- Aplicacoes Streams ----------------------------------------------------
# Rodam no HOST, nao no compose (decisao D6 do desenho de infra): reescrever topologia e
# reiniciar precisa custar segundos, nao um rebuild de imagem. Elas consultam o
# config-server em localhost:8888 no boot -- sem ele de pe, nao sobem.

MVN ?= mvn

# O RocksDB, usado pelos state stores, carrega biblioteca nativa por JNI. Sem esta flag o
# JDK 25 imprime um aviso de metodo restrito a cada boot, e no futuro bloqueia a chamada.
JAVA_FLAGS ?= --enable-native-access=ALL-UNNAMED

apps-build: ## Compila e empacota as apps Streams como jar executavel
	$(MVN) -B -pl orders,antifraud -am package -DskipTests

config: ## Mostra a configuracao que o config-server entrega: make config APP=antifraud [PROFILE=prod]
	@curl -s localhost:8888/lab/$(APP)$(if $(PROFILE),/$(PROFILE)) | python3 -m json.tool

orders: ## Sobe o dominio orders contra o cluster A
	$(MVN) -B -q -pl orders -am package -DskipTests
	APP_NAME=orders java $(JAVA_FLAGS) -jar orders/target/orders-0.0.1.jar

antifraud: ## Sobe o dominio antifraud contra o cluster B
	$(MVN) -B -q -pl antifraud -am package -DskipTests
	APP_NAME=antifraud java $(JAVA_FLAGS) -jar antifraud/target/antifraud-0.0.1.jar

e2e: ## Roda os testes e2e (*IT) -- exige Docker
	$(MVN) -B verify

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

OBSERVABILITY_SERVICES = kafbat dex openobserve otelcol

observability-up: ## Sobe kafbat, dex, openobserve e otelcol
	$(COMPOSE) start $(OBSERVABILITY_SERVICES)

observability-down: ## Para os servicos de observabilidade para liberar RAM
	$(COMPOSE) stop $(OBSERVABILITY_SERVICES)

# O dex roda no namespace de rede do openobserve: reiniciar so um deixa a 5556
# escutando num sandbox orfao, e o login para de responder no host. O kafbat vai
# junto porque o pool de conexoes dele guarda sockets para o Dex que morreram no
# restart -- sem isso, o primeiro login depois daqui falha com 500.
openobserve-restart: ## Reinicia OpenObserve, Dex e kafbat, nessa ordem
	$(COMPOSE) restart openobserve
	$(COMPOSE) restart dex
	$(COMPOSE) restart kafbat

# --- SQL sobre os topicos (Flink) ------------------------------------------

flink-sql: ## Abre o SQL client interativo do Flink
	docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh

flink-example: ## Roda infra/flink/sql/orders.sql -- agrega orders.created por cliente
	docker exec flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/orders.sql

flink-down: ## Para o Flink para liberar RAM
	$(COMPOSE) stop flink-jobmanager flink-taskmanager

flink-up: ## Sobe o Flink de volta
	$(COMPOSE) start flink-jobmanager flink-taskmanager

# --- Inspecao --------------------------------------------------------------

quorum: ## Estado do quorum KRaft: make quorum CLUSTER=a
	docker exec kafka-broker-$(CLUSTER)-1 /opt/kafka/bin/kafka-metadata-quorum.sh \
		--bootstrap-server localhost:9092 describe --status

describe: ## ISR de todos os topicos: make describe CLUSTER=a
	docker exec kafka-broker-$(CLUSTER)-1 /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 --describe
