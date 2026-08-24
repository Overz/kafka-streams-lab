# syntax=docker/dockerfile:1
# API HTTP do lab -- o unico produtor de orders.created que nao e Kafka Streams.
#
# Contexto de build e ESTA pasta: api/ e um modulo Go independente, com go.mod proprio,
# e nao compartilha nada com os modulos Maven.
#
#   docker compose build api      (ou: make build-api na raiz)
#
# Este arquivo nao sabe compilar Go. Como compilar esta em api/Makefile, e daqui so sai
# onde por o binario e para qual plataforma -- entao o binario da imagem e o mesmo que
# "make build" produz na maquina do dev, sem duas listas de flags para manter em sincronia.

FROM golang:1.26-alpine AS build
WORKDIR /src

RUN apk add --no-cache make

# Preenchidos pelo BuildKit, inclusive em "docker buildx --platform". Vazios fora dele, e
# ai o Makefile compila para a plataforma do host.
ARG TARGETOS
ARG TARGETARCH

# bind: o fonte e lido do contexto, sem virar camada. cache: modulos baixados e objetos
# compilados sobrevivem ao build, que e o que a antiga copia do go.mod em separado tentava
# imitar -- e mal, porque qualquer mudanca no go.mod a invalidava.
RUN --mount=type=bind,target=. \
    --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    make build BIN=/out/api GOOS=$TARGETOS GOARCH=$TARGETARCH

# Alpine e nao distroless: o healthcheck da compose usa o wget do busybox, mesmo padrao
# dos outros servicos do lab.
FROM alpine:3.22
WORKDIR /app

# Usuario sem privilegio: o processo so escuta uma porta, nao escreve em disco.
RUN adduser -S -H -D -u 10001 api
USER 10001

COPY --from=build /out/api /app/api

ENV HTTP_PORT=8090
EXPOSE 8090
ENTRYPOINT ["/app/api"]
