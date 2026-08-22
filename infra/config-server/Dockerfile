# Config server do lab. O contexto de build e a RAIZ do projeto (ver context na
# compose.yaml ao lado), nao esta pasta: o modulo precisa do pom.xml agregador e dos
# poms irmaos para resolver o parent, coisa que um contexto local nao alcancaria.
#
# Os arquivos .yaml servidos NAO entram na imagem -- sao montados de
# ./repository em /config. Assim editar configuracao nao exige rebuild.

FROM maven:3.9-eclipse-temurin-25 AS build
WORKDIR /build

# Poms primeiro, sozinhos: a camada de dependencias so invalida quando um pom muda,
# nao a cada linha de Java alterada.
COPY pom.xml .
COPY shared/pom.xml shared/
COPY orders/pom.xml orders/
COPY antifraud/pom.xml antifraud/
COPY config-server/pom.xml config-server/
RUN mvn -B -pl config-server -am dependency:go-offline -DskipTests

COPY config-server/src config-server/src
# -pl com -am so constroi o config-server e o que ele precisa. O modulo nao depende de
# shared, entao orders e antifraud nem chegam a ser compilados aqui.
RUN mvn -B -pl config-server -am package -DskipTests

FROM eclipse-temurin:25-jre
WORKDIR /app

# Usuario sem privilegio: o processo so precisa ler /config, montado read-only.
RUN useradd --system --uid 10001 --create-home config
USER 10001

COPY --from=build /build/config-server/target/config-server-*.jar /app/config-server.jar

EXPOSE 8888
ENTRYPOINT ["java", "-jar", "/app/config-server.jar"]
