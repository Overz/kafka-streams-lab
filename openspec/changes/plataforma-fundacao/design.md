## Context

O lab tem infraestrutura versionada e funcionando, mas zero codigo de aplicacao. Antes de
escrever qualquer topologia e preciso decidir como as duas apps compilam, leem configuracao,
serializam eventos e emitem telemetria -- decisoes que, se tomadas por dominio, produzem
duas respostas diferentes para a mesma pergunta.

Tres restricoes vindas de fora moldam este desenho:

1. **As apps rodam no host, nao no compose** (decisao D6 do desenho de infra). Elas alcancam
   os brokers por `localhost:19092`, o config-server por `localhost:8888` e o collector por
   `localhost:4317`.
2. **Kafka Streams puro, sem Spring nas apps.** Nao ha container de injecao de dependencia,
   nao ha `@Value`, nao ha `@Component`. O grafo de objetos e montado a mao no `main`.
3. **O MirrorMaker 2 usa `DefaultReplicationPolicy`**, entao o topico que o `antifraud` le
   chama-se `cluster-a.orders.fraud-requested` e o que o `orders` le chama-se
   `cluster-b.antifraud.analysis-completed`. Nomes de topico nao podem ser constantes no
   codigo -- precisam vir de configuracao, porque dependem de onde a app roda.

## Goals / Non-Goals

**Goals:**

- Um unico jeito de serializar evento, ler config, iniciar Streams e emitir telemetria,
  compartilhado pelos dois dominios.
- Configuracao errada ou incompleta derruba o processo **no boot**, com o nome da chave na
  mensagem -- nunca um `NullPointerException` na milesima mensagem.
- Os contratos de evento sao a traducao literal do `Kafka Streams Lab.md`, sem campo extra
  e sem campo faltando.
- A base e testavel sem Docker: todo teste deste change roda em `mvn test`.

**Non-Goals:**

- Nenhuma topologia de negocio. Validacao de pedido, calculo de risco e roteamento ficam
  nos changes `pipeline-orders` e `pipeline-antifraud`.
- Nenhum refresh de configuracao em runtime. A config e lida uma vez, no boot. `@RefreshScope`
  e o endpoint `/actuator/refresh` sao problema de outro dia.
- Sem seguranca no config-server (o desenho de infra ja descartou seguranca no lab, D8).
- Sem `exactly_once_v2`. Fica para um change proprio.

## Decisions

| # | Decisao | Alternativas descartadas | Conceito exercitado |
|---|---|---|---|
| D1 | **Config em records tipados** desserializados por Jackson atras da interface `AppConfig`. Cada secao do YAML vira um record com `@Jacksonized @Builder @With`. | `Map<String,Object>` + getters por chave string -- erro de digitacao so aparece em runtime, e `getList(K, String.class)` nao da garantia nenhuma ao compilador. Variaveis de ambiente -- ignora o config-server, que e requisito. | — |
| D2 | **Backend `native` no config-server**, lendo `.yaml` de um diretorio montado. | Backend Git -- exige um repositorio so para guardar quatro arquivos, e o exercicio de versionar config ja e coberto pelo proprio repo do lab. | — |
| D3 | **`ConfigServerClient` usa o `HttpClient` do JDK**, sem cliente HTTP externo. Le `GET /{app}/{profile}` e achata `propertySources` na ordem em que o Spring os devolve (o primeiro elemento tem a maior precedencia). | `spring-cloud-starter-config` nas apps -- arrastaria o Spring inteiro para dentro de um lab cujo ponto e Kafka Streams puro. OkHttp/Apache HttpClient -- dependencia nova para um unico GET. | — |
| D4 | **O JSON do config-server e achatado em chaves pontuadas e depois re-aninhado** antes de virar record. O config-server devolve `{"kafka.bootstrap-servers": "a,b,c"}`, nao `{"kafka":{"bootstrap-servers":...}}`. | Pedir `/{app}-{profile}.json` (que ja vem aninhado) -- perde a resolucao de precedencia entre `application.yaml` e `orders.yaml`, que e justamente o que o config-server faz de util. | — |
| D5 | **`JsonSerde<T>` compartilha um unico `ObjectMapper`** configurado com `JavaTimeModule`, `WRITE_DATES_AS_TIMESTAMPS=false` e `FAIL_ON_UNKNOWN_PROPERTIES=false`. Desconhecido tolerado na leitura permite evoluir o produtor antes do consumidor. | Um mapper por serde -- desperdicio e risco de divergencia de config. `FAIL_ON_UNKNOWN=true` -- travaria o lab a cada campo novo. | — |
| D6 | **Valores monetarios em `BigDecimal`**, nunca `double`. O `totalAmount` precisa bater exatamente com `Σ(quantity × unitPrice)`, e `0.1 + 0.2 != 0.3` em ponto flutuante. A comparacao usa `compareTo` com escala normalizada, nao `equals`. | `double` -- a regra de validacao do §3 falharia de forma intermitente. `long` em centavos -- correto, mas divergiria do JSON do documento de dominio. | — |
| D7 | **Erro de etapa e valor, nao excecao**: `sealed interface StageOutcome<T>` com `Success<T>` e `Failure`. Uma etapa que rejeita um evento devolve `Failure`; a topologia roteia com `split()` sem saber o motivo. | Lancar excecao -- em Kafka Streams uma excecao nao capturada mata a thread da task, e o comportamento correto para um pedido invalido e ir para a DLQ, nao derrubar a aplicacao. | branch/split |
| D8 | **Headers auxiliares tem prefixo `x-internal-` e sao removidos por um `FixedKeyProcessor` dedicado** imediatamente antes de cada sink. Um unico ponto de limpeza, aplicado a todos os ramos inclusive a DLQ. | Limpar dentro de cada etapa -- garantido esquecer em um ramo. Nao usar headers auxiliares -- perde a forma natural de carregar contexto entre etapas sem poluir o payload. | Processor API, `FixedKeyProcessor` |
| D9 | **`FixedKeyProcessor` onde a chave nao muda**, `Processor` so onde muda. O tipo `FixedKeyProcessor` impede fisicamente a reparticao acidental -- ele nao tem como emitir uma chave diferente. | `Processor` em tudo -- funciona, mas perde uma garantia que o compilador poderia dar de graca. | Processor API |
| D10 | **Telemetria via SDK no codigo**, com `AutoConfiguredOpenTelemetrySdk`. As metricas nativas do Kafka Streams sao expostas por uma ponte que registra `KafkaStreams.metrics()` como `ObservableDoubleGauge`. | `-javaagent` -- invisivel nos testes e exige o jar presente para qualquer trace existir. Sem telemetria -- o otelcol ja esta de pe esperando OTLP na 4317. | — |
| D11 | **`StreamsRunner` implementa `AutoCloseable` e registra o shutdown hook**, com `CountDownLatch` para segurar o `main`. Fecha com timeout explicito. | `streams.start()` solto no `main` -- `Ctrl+C` mata a app no meio de um commit, e o proximo boot reprocessa. | ciclo de vida |
| D12 | **Nome de topico e de state store vem da config**, nunca de constante Java. Sem isso o `antifraud` nao teria como saber que le `cluster-a.orders.fraud-requested` num ambiente e `orders.fraud-requested` em outro (o e2e da camada 1 roda com um broker so). | Constantes em `Topics.java` -- forcaria o teste e2e a subir MirrorMaker 2 so para ter o prefixo certo. | — |

### Forma do `AppConfig`

```java
public interface AppConfig {
  KafkaConfig kafka();
  TopicsConfig topics();
}
```

`orders` e `antifraud` estendem essa interface com o que so eles precisam
(`AntifraudConfig extends AppConfig { FraudRulesConfig rules(); }`), de modo que o `shared`
nao conhece regra de fraude e o `orders` nao carrega config que nao usa.

### Ordem dos annotation processors

`annotationProcessorPaths` precisa listar, nesta ordem: `lombok`,
`lombok-mapstruct-binding`, `mapstruct-processor`. Fora dessa ordem o MapStruct roda antes
do Lombok gerar os acessores e os mappers saem vazios, sem erro de compilacao -- falha
silenciosa que so aparece em runtime.

## Risks / Trade-offs

- **Config-server fora do ar impede o boot das apps.** → Aceito e deliberado: e melhor falhar
  no boot do que subir com config parcial. Mitigado por retry com backoff limitado no
  `ConfigServerClient` (o container pode ainda estar subindo) e mensagem de erro que nomeia
  a URL tentada.
- **`@Jacksonized` na Lombok 1.18.44+ emite warning ate se declarar o alvo Jackson 2 ou 3.**
  → Fixar a chave correspondente no `lombok.config` da raiz; sem isso todo record gera ruido
  no build.
- **Testcontainers 2.x renomeou os artifactIds** (`testcontainers-kafka`, nao `kafka`). →
  Registrado aqui e no `dependencyManagement`; um `pom.xml` copiado de tutorial da 1.x nao resolve.
- **`FAIL_ON_UNKNOWN_PROPERTIES=false` esconde erro de digitacao em nome de campo.** → Trade-off
  aceito para permitir evolucao independente de produtor e consumidor; compensado por testes de
  round-trip que comparam o record inteiro, nao campo a campo.
- **Achatar e re-aninhar o JSON do config-server e codigo nao trivial no caminho do boot.**
  → Coberto por teste contra um `HttpServer` do JDK devolvendo a resposta real do config-server,
  incluindo dois `propertySources` com precedencia conflitante.

## Open Questions

- O nome exato da chave do `lombok.config` que fixa o alvo do `@Jacksonized` em Jackson 2 foi
  introduzido na 1.18.44 e sera confirmado no changelog durante a implementacao.
- Se o `StreamsMetricsBridge` deve exportar todas as metricas do Streams ou um subconjunto.
  Comeca com todas; se o volume no OpenObserve incomodar, vira lista de prefixos na config.
