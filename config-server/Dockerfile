# syntax=docker/dockerfile:1
# Config server do lab, e quem cria os topicos dos dois clusters no boot.
#
# O contexto de build e a RAIZ do projeto (ver context na compose.yaml ao lado), nao esta
# pasta: o modulo precisa do pom.xml agregador e dos poms irmaos para resolver o parent,
# coisa que um contexto local nao alcancaria. O .dockerignore da raiz mantem o contexto
# pequeno -- .git, target/, infra/, docs/ e .data/ ficam de fora.
#
#   docker compose build config-server      (ou: make build-config-server)
#
# Os .yaml de configuracao servidos NAO entram na imagem -- sao montados de
# ./repository em /config. Ja os de topico (src/main/resources/kafka/) entram, porque
# viajam no jar.
#
# Nao ha copia de poms em separado nem dependency:go-offline: o cache mount do /root/.m2
# guarda o repositorio Maven entre builds, e ao contrario do truque de ordem de camada ele
# nao e invalidado quando um pom muda.

FROM maven:3.9-eclipse-temurin-25 AS build
WORKDIR /build

COPY . .

# -pl com -am so constroi o config-server e o que ele precisa. O modulo nao depende de
# shared, entao orders e antifraud nem chegam a ser compilados aqui.
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -pl config-server -am package -DskipTests

FROM eclipse-temurin:25-jre
WORKDIR /app

# Usuario sem privilegio: o processo so precisa ler /config, montado read-only.
RUN useradd --system --uid 10001 --create-home config
USER 10001

COPY --from=build /build/config-server/target/config-server-*.jar /app/config-server.jar

EXPOSE 8888
ENTRYPOINT ["java", "-jar", "/app/config-server.jar"]
