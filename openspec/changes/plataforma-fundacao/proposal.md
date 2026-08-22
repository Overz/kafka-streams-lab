## Why

Os tres modulos Java do lab estao vazios: `orders` e `antifraud` contem apenas o `Main.java`
de template do IntelliJ, `shared` nao tem uma unica classe, e os tres `pom.xml` nao declaram
nenhuma dependencia. Nao existe forma de compilar uma topologia Kafka Streams, ler
configuracao, serializar um evento ou emitir um trace.

A infraestrutura ja esta pronta e rodando (dois clusters KRaft, MirrorMaker 2, otelcol,
OpenObserve), mas nao ha nada do lado da aplicacao para conversar com ela. Este change
constroi essa base -- e so ela -- para que os pipelines de `orders` e `antifraud` possam
ser escritos em cima de contratos ja testados, em vez de cada dominio inventar o seu.

## What Changes

- **Build**: `pom.xml` raiz ganha `dependencyManagement` com todos os BOMs, o
  `maven-compiler-plugin` com a cadeia de annotation processors Lombok -> MapStruct,
  e a separacao surefire (unitario) / failsafe (e2e). `lombok.config` na raiz.
- **Novo modulo `config-server`**: Spring Cloud Config Server com backend `native`,
  servindo os `.yaml` de `infra/config-server/repository/`. Sobe como container no
  `docker-compose.yaml` da raiz, na porta 8888.
- **Contratos de evento no `shared`**: os seis eventos do documento de dominio como
  Java records -- `OrderCreated`, `OrderItem`, `OrderValidated`, `OrderForFraudAnalysis`,
  `FraudAnalysisCompleted`, `OrderProcessed` -- mais `DeadLetter`, `Decision` e `FraudReason`.
- **Serializacao**: `JsonSerde<T>` implementando `org.apache.kafka.common.serialization.Serde`,
  com Jackson configurado uma unica vez para o lab inteiro.
- **Leitura de configuracao**: `ConfigServerClient` busca `GET /{app}/{profile}` no boot,
  achata os `propertySources` na ordem de precedencia do Spring e desserializa em records
  tipados atras da interface `AppConfig`. Chave obrigatoria ausente derruba o processo no
  boot, nomeando a chave.
- **Ciclo de vida do Streams**: `StreamsRunner` recebe `Topology` e `Properties` e cuida de
  start, shutdown hook, `StateListener` e `UncaughtExceptionHandler`.
- **Headers**: constantes em `Headers` e o `HeaderCleanupProcessor`, um
  `FixedKeyProcessor` que remove todo header com prefixo `x-internal-` antes de qualquer sink.
- **Telemetria**: SDK do OpenTelemetry via `AutoConfiguredOpenTelemetrySdk`, exportando OTLP
  gRPC para `localhost:4317`; `OpenTelemetryAppender` no Logback; `StreamsMetricsBridge`
  publicando `KafkaStreams.metrics()` como gauges OTel.
- **Remocao**: `shared/src/main/java/com/github/overz/Main.java`, template do IntelliJ.

Nenhuma topologia de negocio e escrita neste change. `orders` e `antifraud` continuam sem
pipeline ate os changes seguintes.

## Capabilities

### New Capabilities

- `build-toolchain`: como o projeto compila -- Java 25, ordem dos annotation processors,
  separacao entre testes unitarios e e2e, e de onde vem cada versao de dependencia.
- `shared-contracts`: os eventos do dominio como records, sua representacao JSON no
  Kafka, e a serializacao/desserializacao entre eles e os bytes do topico.
- `runtime-config`: como `orders` e `antifraud` obtem sua configuracao do config-server
  no boot e o que acontece quando ela esta incompleta ou o servidor esta fora.
- `streams-runtime`: o ciclo de vida de uma aplicacao Streams do lab -- start, parada
  limpa, tratamento de excecao nao capturada -- e a disciplina de headers auxiliares.
- `observability`: o que cada aplicacao emite em trace, log e metrica, e para onde.

### Modified Capabilities

Nenhuma. Este e o primeiro change do projeto.

## Impact

**Codigo novo**: modulo `config-server`; pacotes `config`, `event`, `serde`, `streams` e
`telemetry` em `shared`.

**Codigo alterado**: `pom.xml` da raiz e dos tres modulos; `docker-compose.yaml` da raiz
ganha um `include`.

**Codigo removido**: `shared/src/main/java/com/github/overz/Main.java`.

**Infraestrutura**: nova pasta `infra/config-server/` (compose, Dockerfile, repository).
O container do config-server passa a ser dependencia de boot das duas apps -- sem ele,
nem `orders` nem `antifraud` sobem.

**Dependencias novas**: kafka-streams 4.3.1, Lombok 1.18.46, MapStruct 1.6.3,
Jackson 2.22.2, SLF4J 2.0.18, Logback 1.6.3, OpenTelemetry 1.65.0 (+ instrumentacao
2.31.0-alpha), JUnit 6.1.3, AssertJ 3.27.7, Cucumber 7.34.7, Testcontainers 2.0.5,
Spring Boot 4.0.8 e Spring Cloud 2025.1.3 (estes dois so no `config-server`).

**Portas**: 8888 passa a ser usada pelo config-server no host.
