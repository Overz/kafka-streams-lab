## ADDED Requirements

### Requirement: Compilacao em Java 25
Todos os modulos Java do projeto SHALL compilar com release 25, encoding UTF-8, e SHALL herdar
essa configuracao do `pom.xml` da raiz sem redeclara-la.

#### Scenario: Build limpo do projeto inteiro
- **WHEN** `mvn clean verify -DskipITs` roda na raiz
- **THEN** os modulos `shared`, `orders`, `antifraud` e `config-server` compilam sem erro
- **AND** nenhum modulo declara `maven.compiler.source` ou `maven.compiler.target` no proprio pom

#### Scenario: Codigo que exige Java 25
- **WHEN** o codigo usa `sealed interface` com records aninhados e pattern matching em `switch`
- **THEN** a compilacao aceita sem `--enable-preview`

### Requirement: Cadeia de annotation processors
O `maven-compiler-plugin` SHALL declarar `annotationProcessorPaths` contendo, nesta ordem
exata: `lombok`, `lombok-mapstruct-binding`, `mapstruct-processor`. A ordem e normativa
porque o MapStruct precisa enxergar os acessores que o Lombok gera.

#### Scenario: Mapper MapStruct sobre record com Lombok
- **WHEN** um `@Mapper` do MapStruct converte um record anotado com `@Builder`
- **THEN** a implementacao gerada popula todos os campos do destino
- **AND** nao ha metodo gerado com corpo vazio

### Requirement: Versoes centralizadas
O `pom.xml` da raiz SHALL declarar todas as versoes de dependencia em `dependencyManagement`,
via BOM sempre que o fornecedor publicar um. Nenhum modulo filho SHALL declarar versao.

#### Scenario: Modulo filho adiciona dependencia
- **WHEN** `orders/pom.xml` declara uma dependencia ja gerenciada na raiz
- **THEN** a declaracao contem apenas `groupId` e `artifactId`
- **AND** `mvn dependency:tree` resolve a versao definida no BOM da raiz

#### Scenario: Artifact renomeado no Testcontainers 2.x
- **WHEN** o projeto declara o modulo Kafka do Testcontainers
- **THEN** o artifactId e `testcontainers-kafka`, nao `kafka`
- **AND** o modulo JUnit e `testcontainers-junit-jupiter`, nao `junit-jupiter`

### Requirement: Separacao entre teste unitario e teste e2e
O build SHALL executar testes unitarios no surefire e testes e2e no failsafe. Testes unitarios
SHALL rodar sem Docker.

#### Scenario: Build sem Docker disponivel
- **WHEN** `mvn verify -DskipITs` roda numa maquina sem Docker
- **THEN** todos os testes unitarios executam e passam
- **AND** nenhum teste tenta abrir socket para um broker Kafka

#### Scenario: Nomenclatura decide o executor
- **WHEN** uma classe de teste termina em `Test`
- **THEN** o surefire a executa
- **WHEN** uma classe de teste termina em `IT`
- **THEN** o failsafe a executa
