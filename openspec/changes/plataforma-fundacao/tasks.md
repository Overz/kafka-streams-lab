## 1. Build e toolchain

- [x] 1.1 Escrever `lombok.config` na raiz com `addLombokGeneratedAnnotation`, `stopBubbling` e a chave que fixa o alvo do `@Jacksonized` em Jackson 2 (confirmar o nome exato no changelog da 1.18.44)
- [x] 1.2 Reescrever o `pom.xml` da raiz: `dependencyManagement` com os BOMs (Kafka, Jackson, OTel, JUnit, Cucumber, Testcontainers), `maven-compiler-plugin` 3.15.0 com `annotationProcessorPaths` na ordem lombok -> lombok-mapstruct-binding -> mapstruct-processor, surefire 3.5.6 e failsafe 3.5.6
- [x] 1.3 Remover a dependencia incondicional de `shared` do `<dependencies>` da raiz (o `config-server` nao precisa dela) e declara-la em `orders` e `antifraud`
- [x] 1.4 Adicionar o modulo `config-server` ao `<modules>` da raiz
- [x] 1.5 Preencher `shared/pom.xml` com kafka-streams, jackson, slf4j, logback, otel, lombok, mapstruct e as dependencias de teste
- [x] 1.6 Apagar `shared/src/main/java/com/github/overz/Main.java`
- [x] 1.7 Verificar: `mvn clean compile` passa nos quatro modulos

## 2. Contratos de evento (shared)

- [x] 2.1 Teste: round-trip do `JsonSerde` para cada evento, instante em ISO-8601, campo desconhecido ignorado, payload corrompido, tombstone
- [x] 2.2 Teste: `compareTo` sobre `BigDecimal` com escalas diferentes; soma de itens sem erro de ponto flutuante
- [x] 2.3 Teste: `@With` devolve nova instancia e preserva a original
- [x] 2.4 Implementar os enums `Decision` e `FraudReason`
- [x] 2.5 Implementar os records `OrderItem`, `OrderCreated`, `OrderValidated`, `OrderForFraudAnalysis`, `FraudAnalysisCompleted`, `OrderProcessed` e `DeadLetter`
- [x] 2.6 Implementar `StageOutcome` como `sealed interface` com `Success` e `Failure`
- [x] 2.7 Implementar `JsonSerde` e a fabrica `JsonSerdes` com o `ObjectMapper` unico do lab
- [x] 2.8 Verificar: `mvn -pl shared test` verde

## 3. Config-server

- [x] 3.1 Criar o modulo `config-server` (Spring Boot 4.0.8 + Spring Cloud 2025.1.3, `spring-cloud-config-server`)
- [x] 3.2 `application.yaml` do servidor: perfil `native`, `search-locations: file:/config`, porta 8888
- [x] 3.3 Escrever `infra/config-server/repository/application.yaml`, `orders.yaml` e `antifraud.yaml` com brokers, topicos, stores, janelas e limiares das regras de fraude
- [x] 3.4 `infra/config-server/Dockerfile` e `infra/config-server/compose.yaml`, montando `repository/` em `/config`
- [x] 3.5 Incluir `infra/config-server/compose.yaml` no `docker-compose.yaml` da raiz
- [x] 3.6 Verificar: `docker compose up -d config-server` e `curl -s localhost:8888/orders/local` devolve os dois `propertySources` na ordem certa

## 4. Leitura de configuracao (shared)

- [x] 4.1 Teste: `ConfigServerClient` contra um `HttpServer` do JDK devolvendo a resposta real do config-server, com duas fontes em conflito, cobrindo precedencia, chave aninhada e lista de um elemento
- [x] 4.2 Teste: chave obrigatoria ausente, tipo incompativel e servidor indisponivel derrubam o boot com mensagem nomeando chave e URL
- [x] 4.3 Implementar `ConfigServerClient` com o `HttpClient` do JDK, achatamento e re-aninhamento das `propertySources`, e retry com backoff limitado
- [x] 4.4 Implementar a interface `AppConfig` e os records `KafkaConfig` e `TopicsConfig`
- [x] 4.5 Verificar: `mvn -pl shared test` verde

## 5. Runtime de Streams (shared)

- [x] 5.1 Teste: `HeaderCleanupProcessor` com `TopologyTestDriver` -- remove todo `x-internal-`, preserva `x-request-id` e `traceparent`, preserva a chave
- [x] 5.2 Teste: `StreamsRunner` registra transicao de estado e fecha no shutdown
- [x] 5.3 Implementar `Headers` com as constantes de prefixo e nomes
- [x] 5.4 Implementar `HeaderCleanupProcessor` como `FixedKeyProcessor`
- [x] 5.5 Implementar `TopologySupplier` e `StreamsRunner` com shutdown hook, `StateListener`, `UncaughtExceptionHandler` e `CountDownLatch`
- [x] 5.6 Verificar: `mvn -pl shared test` verde

## 6. Telemetria (shared)

- [x] 6.1 Teste: `StreamsMetricsBridge` converte metrica numerica em gauge e ignora metrica nao numerica, usando `InMemoryMetricReader`
- [x] 6.2 Teste: span de etapa encerra com status de erro e carrega o motivo, usando o exportador em memoria do SDK de teste
- [x] 6.3 Implementar `OpenTelemetryBootstrap` com `AutoConfiguredOpenTelemetrySdk` e o recurso `service.name`
- [x] 6.4 Implementar `Telemetry` (abertura e fechamento de span por etapa) e `StreamsMetricsBridge`
- [x] 6.5 Configurar `logback.xml` com `OpenTelemetryAppender` e um encoder legivel no console
- [x] 6.6 Verificar: `mvn -pl shared test` verde

## 7. Verificacao do change

- [x] 7.1 `mvn clean verify -DskipITs` verde no projeto inteiro
- [x] 7.2 `docker compose up -d --wait` sobe com o config-server saudavel
- [x] 7.3 `openspec validate plataforma-fundacao --strict` sem erro
