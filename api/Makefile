# Build da API.
#
# Serve para a maquina do dev e para dentro da imagem: o Dockerfile chama "make build" em
# vez de repetir a linha do go build, entao as flags vivem num lugar so. Mudar -ldflags
# aqui muda o binario nos dois caminhos, e nao ha como um sair diferente do outro.
#
#   make build                          binario em tmp/api/api, na raiz do repo
#   make build BIN=/out/api             onde o Dockerfile o quer
#   make build GOOS=linux GOARCH=arm64  cross-compile
#   make run                            sobe no host, contra os listeners EXTERNAL

GO  ?= go
PKG ?= ./cmd/api

# tmp/ da raiz do repositorio: saida de build e descartavel, e mora junto com o resto do
# que da para apagar sem perder nada. O Dockerfile sobrepoe com BIN=/out/api.
TMP ?= ../tmp
BIN ?= $(TMP)/api/api

# Estatico por padrao: o binario roda em qualquer base, sem libc.
CGO_ENABLED ?= 0

# Vazios = plataforma do host. O Dockerfile passa TARGETOS/TARGETARCH do BuildKit aqui.
GOOS   ?=
GOARCH ?=

# -trimpath tira o caminho de build de dentro do binario.
GOFLAGS ?= -trimpath
# -s -w descartam tabela de simbolos e DWARF: alguns MB a menos, sem afetar o stack trace
# de um panic.
LDFLAGS ?= -s -w

# O $(if ...) e o que deixa "GOOS=" vazio nao virar GOOS='' na linha de comando -- e o que
# chega quando o Dockerfile roda fora do BuildKit e TARGETOS nao esta preenchido.
GO_ENV = CGO_ENABLED=$(CGO_ENABLED)$(if $(GOOS), GOOS=$(GOOS))$(if $(GOARCH), GOARCH=$(GOARCH))

# Os tres enderecos EXTERNAL do cluster A: rodando no host, kafka-broker-a-1:9092 nao
# resolve. Dentro do compose quem manda sao as variaveis de api/compose.yaml.
RUN_ENV = KAFKA_BROKERS=localhost:19092,localhost:19093,localhost:19094 \
          ORDER_INPUT_TOPIC=orders.created

.DEFAULT_GOAL := help
.PHONY: help build run test vet fmt tidy check clean

help: ## Lista os alvos
	@grep -hE '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

build: ## Compila o binario: make build [BIN=caminho] [GOOS=linux GOARCH=arm64]
	$(GO_ENV) $(GO) build $(GOFLAGS) -ldflags="$(LDFLAGS)" -o $(BIN) $(PKG)

run: ## Sobe a API no host, contra o cluster A
	$(RUN_ENV) $(GO) run $(PKG)

test: ## Roda os testes
	$(GO) test ./...

vet: ## Analise estatica do proprio toolchain
	$(GO) vet ./...

fmt: ## Formata o codigo
	$(GO) fmt ./...

tidy: ## Sincroniza go.mod e go.sum com os imports
	$(GO) mod tidy

check: fmt vet test ## fmt + vet + test

clean: ## Remove o binario
	rm -rf $(TMP)/api
