# Kafka multi-cluster HA — design

Data: 2026-08-07
Status: aprovado, aguardando plano de implementação

## Objetivo

Substituir a ausência de infraestrutura do `kafka-streams-lab` por **dois clusters Kafka
independentes, cada um em alta disponibilidade, replicando entre si via MirrorMaker 2**.

A motivação é didática: aprender gerenciamento de Kafka multi-cluster —
quorum KRaft, fator de replicação, `min.insync.replicas`, replicação assíncrona
entre sites e o que quebra quando um nó cai.

O desenho não é arbitrário: ele mapeia os dois domínios que o lab já define
(`orders` e `antifraud`) para os dois clusters, de forma que a fronteira de
replicação coincida com a fronteira de domínio.

## Decisões

| # | Decisão | Alternativas descartadas |
|---|---|---|
| D1 | **Active/active por domínio**: cluster A hospeda `orders`, cluster B hospeda `antifraud`. Cada app Streams fala só com o cluster local. | Active/passive (DR) — não usa a divisão de domínios que o lab já tem. |
| D2 | **Controllers e brokers em processos separados**: 3 + 3 por cluster. | Papéis combinados (3 nós/cluster) — perde a lição de plano de controle falhando independente do plano de dados. |
| D3 | **Redes Docker isoladas** (`site-a`, `site-b`), sem rota entre elas. MM2, kafbat e o collector são as únicas travessias. | Rede única — permitiria uma app falar com o cluster errado sem erro visível. |
| D4 | **MM2 em dedicated mode, 2 instâncias**. | Connect distribuído + REST (mais artefatos antes do primeiro byte replicado); dois processos unidirecionais (nenhum HA sozinho). |
| D5 | **Observabilidade**: kafbat (estado atual) + `grafana/otel-lgtm` (histórico), mesmo padrão de `spring-graph-rag`. | Prometheus + Grafana separados — mais containers para o mesmo resultado. |
| D6 | **Apps rodam no host**, não no compose. | Containerizar as apps — atrapalha o ciclo de reescrever código Streams. |
| D7 | **Quorum KRaft estático** (`controller.quorum.voters`). | Quorum dinâmico do 4.x — melhor exercício, fica para depois; o estático deixa o quorum legível num arquivo enquanto se aprende a forma. |
| D8 | **Sem segurança** (PLAINTEXT em tudo). | SASL/SCRAM + TLS — muito tempo depurando config antes de aprender o que interessa. Fica fora do escopo. |

## Ponto de partida

`~/dev/github/java/spring-camel-kafka/infra/kafka/compose.yaml` serve de base
estrutural (anchors YAML, `env_file` por papel, apache/kafka 4.1.1), mas **não é
HA** e não pode ser copiado como está. Três defeitos precisam ser corrigidos:

| Defeito no original | Efeito | Correção |
|---|---|---|
| `KAFKA_CONTROLLER_QUORUM_VOTERS` com 2 votantes | Maioria de 2 em 2 votantes: perder 1 controller congela o plano de controle | 3 controllers por cluster |
| Volume nomeado único (`kafka`) montado nos 4 nós | Cada nó grava `meta.properties` com o próprio `node.id` no log dir; compartilhar corrompe | 1 volume nomeado por nó |
| `KAFKA_LOG_DIRS=/var/lib/kafka/logs` mas o volume monta em `/var/lib/kafka/data` | Dados vivem no layer efêmero do container: `down` apaga tudo, o volume fica vazio para sempre | Alinhar `KAFKA_LOG_DIRS` ao mount |

Faltavam também: `min.insync.replicas`, `default.replication.factor`,
`unclean.leader.election.enable=false` e RF=3 nos tópicos internos
(`__consumer_offsets`, `__transaction_state`).

## Arquitetura

```
        rede docker: site-a                          rede docker: site-b
┌────────────────────────────────┐          ┌────────────────────────────────┐
│ controller-a-1  node.id 1      │          │ controller-b-1  node.id 1      │
│ controller-a-2  node.id 2      │          │ controller-b-2  node.id 2      │
│ controller-a-3  node.id 3      │          │ controller-b-3  node.id 3      │
│   quorum KRaft :9093           │          │   quorum KRaft :9093           │
│                                │          │                                │
│ broker-a-1  node.id 11  :19092 │          │ broker-b-1  node.id 11  :29092 │
│ broker-a-2  node.id 12  :19093 │          │ broker-b-2  node.id 12  :29093 │
│ broker-a-3  node.id 13  :19094 │          │ broker-b-3  node.id 13  :29094 │
└────────────┬───────────────────┘          └───────────────┬────────────────┘
             │                                              │
             └──────────┬──────────────────────┬────────────┘
                        │                      │
                 mirror-1, mirror-2      kafbat :8080
                 (MM2 dedicated)         lgtm   :3000
                 nas duas redes          nas duas redes
```

Os `node.id` se repetem entre os sites de propósito — são clusters distintos,
com `CLUSTER_ID` diferente. Isso mantém os `.env` simétricos: a única diferença
entre `site-a/common.env` e `site-b/common.env` é o `CLUSTER_ID` e os hostnames
do quorum.

`CLUSTER_ID` gerados com `kafka-storage random-uuid`:

- site-a: `jCRZjivpSlC9UCHGEccVuw`
- site-b: `c4rUZKUKRzqJyLC9N6kpcw`

Os apps rodam no host (`./mvnw`, `go run`), por isso cada broker publica um
listener `EXTERNAL`: `orders` aponta para `localhost:19092,19093,19094` e
`antifraud` para `localhost:29092,29093,29094`.

### Orçamento de memória

Host: 31Gi total, ~12Gi livres. Heap capado por nó (controller 320m, broker 512m).

| Componente | Limite unitário | Total |
|---|---|---|
| 6 controllers | 448Mb | 2,6Gi |
| 6 brokers | 768Mb | 4,6Gi |
| 2 MM2 | 768Mb | 1,5Gi |
| kafbat | 512Mb | 0,5Gi |
| grafana-lgtm | 1,5Gb | 1,5Gi |
| **Total** | | **~10,7Gi** |

Cabe, mas sem folga grande quando as duas JVMs de Streams também estiverem
rodando. `observability.yaml` sobe separado justamente para poder ficar fora
quando não se está medindo.

## Configuração de HA

`site-{a,b}/common.env`:

```properties
KAFKA_LOG_DIRS=/var/lib/kafka/data
KAFKA_NUM_PARTITIONS=3
KAFKA_DEFAULT_REPLICATION_FACTOR=3
KAFKA_MIN_INSYNC_REPLICAS=2
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=3
KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=3
KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=2
KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR=3
KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR=2
KAFKA_UNCLEAN_LEADER_ELECTION_ENABLE=false
KAFKA_AUTO_CREATE_TOPICS_ENABLE=false
KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0
KAFKA_OPTS=-javaagent:/opt/jmx/jmx_prometheus_javaagent.jar=7071:/opt/jmx/kafka.yml

# as duas únicas linhas que diferem entre site-a e site-b:
CLUSTER_ID=jCRZjivpSlC9UCHGEccVuw
KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka-controller-a-1:9093,2@kafka-controller-a-2:9093,3@kafka-controller-a-3:9093
```

A garantia real vem do par **`min.insync.replicas=2` + `acks=all` no producer**:
com RF=3, perder 1 broker não interrompe a escrita; perder 2 interrompe — e esse
é o comportamento correto, porque escrever com 1 réplica é perder dado
silenciosamente na próxima falha.

`unclean.leader.election.enable=false` completa a garantia: prefere ficar
indisponível a eleger um líder atrasado e descartar mensagens já confirmadas.

`auto.create.topics.enable=false` é deliberado — tópico criado automaticamente
nasce com o default do broker e ninguém percebe se o RF saiu errado. Tópicos são
criados explicitamente por `scripts/create-topics.sh`.

## MirrorMaker 2

### Tópicos

| Site | Tópico | Origem |
|---|---|---|
| A | `orders.created` | API Go / produtor externo |
| A | `orders.validated` | app `orders` |
| A | `orders.fraud-requested` | app `orders` — **replicado para B** |
| A | `orders.processed` | app `orders` |
| A | `site-b.antifraud.analysis-completed` | MM2, vindo de B |
| B | `site-a.orders.fraud-requested` | MM2, vindo de A |
| B | `antifraud.analysis-completed` | app `antifraud` — **replicado para A** |

### `infra/kafka/mirror/mm2.properties`

```properties
clusters = site-a, site-b
site-a.bootstrap.servers = kafka-broker-a-1:9092,kafka-broker-a-2:9092,kafka-broker-a-3:9092
site-b.bootstrap.servers = kafka-broker-b-1:9092,kafka-broker-b-2:9092,kafka-broker-b-3:9092

site-a->site-b.enabled = true
site-a->site-b.topics  = orders\.fraud-requested
site-b->site-a.enabled = true
site-b->site-a.topics  = antifraud\.analysis-completed

replication.policy.class = org.apache.kafka.connect.mirror.DefaultReplicationPolicy
tasks.max = 3

replication.factor                    = 3
checkpoints.topic.replication.factor  = 3
heartbeats.topic.replication.factor   = 3
offset-syncs.topic.replication.factor = 3
offset.storage.replication.factor     = 3
status.storage.replication.factor     = 3
config.storage.replication.factor     = 3

sync.topic.configs.enabled      = true
sync.group.offsets.enabled      = true
emit.checkpoints.enabled        = true
emit.heartbeats.enabled         = true
refresh.topics.interval.seconds = 10
```

`tasks.max = 3` é o que faz as duas instâncias dividirem trabalho. Com o default
`1`, uma instância ficaria ociosa e a HA da camada de replicação não seria
observável.

### Por que o prefixo existe

`DefaultReplicationPolicy` renomeia o tópico remoto para `<cluster-origem>.<tópico>`.
Não é cosmético: **impede loop infinito**. Em replicação bidirecional com nomes
iguais, o fluxo A→B copiaria `orders.fraud-requested` para B e o fluxo B→A
copiaria de volta, indefinidamente. Com prefixo, o MM2 reconhece que
`site-a.orders.fraud-requested` já é remoto e não o replica de volta.

`IdentityReplicationPolicy` evita o prefixo, mas só é segura em active/passive
unidirecional.

### Consequências para as apps Streams

```java
// antifraud — consome remoto, produz local
builder.stream("site-a.orders.fraud-requested", ...)
       .to("antifraud.analysis-completed");

// orders — produz local, consome remoto
builder.stream("site-b.antifraud.analysis-completed", ...)
```

**1. `exactly_once_v2` não atravessa cluster.** EOS é uma garantia por cluster:
a transação abrange consumo, mudança de state store e produção dentro do mesmo
cluster. O MM2 é um consumer comum lendo de A e um producer comum escrevendo em
B — **at-least-once**. Duplicatas cruzam a fronteira. A regra de idempotência da
seção 13 de `Kafka Streams Lab.md` deixa de ser exercício opcional e vira
requisito estrutural.

**2. Co-partitioning sobrevive.** O MM2 cria o tópico remoto com o mesmo número
de partições e preserva a partição de origem de cada registro. Join por
`orderId` continua válido, desde que o tópico local do outro lado do join também
tenha 3 partições e a mesma chave. Mudar `num.partitions` em um site só quebra
os joins silenciosamente.

**3. Offsets não são comparáveis entre clusters.** Offset 42 em A não é offset 42
em B. `sync.group.offsets.enabled` faz o MM2 emitir checkpoints traduzindo
posições de consumer group — irrelevante no fluxo normal, essencial em failover.

**4. Lag de replicação é o RPO.** Site B fora do ar: `orders` continua
produzindo, o MM2 acumula lag, nada se perde (está durável em A). Quando B volta,
o antifraude processa o passado. Essa é a diferença entre "dois clusters" e "um
cluster esticado".

## Observabilidade

Duas peças, ambas nas duas redes.

**kafbat** com os dois clusters registrados — estado atual: ISR por partição,
partições under-replicated, controller ativo, tópicos com prefixo aparecendo do
outro lado, lag do consumer group do MM2.

**grafana/otel-lgtm** com `otelcol-config.yaml` montado — histórico temporal. O
collector raspa 14 targets:

```yaml
  prometheus/collector:
    config:
      scrape_configs:
        - job_name: "kafka-site-a"
          scrape_interval: 15s
          static_configs:
            - targets:
                - "kafka-controller-a-1:7071"
                - "kafka-controller-a-2:7071"
                - "kafka-controller-a-3:7071"
                - "kafka-broker-a-1:7071"
                - "kafka-broker-a-2:7071"
                - "kafka-broker-a-3:7071"
              labels: { site: "a" }
        - job_name: "kafka-site-b"
          scrape_interval: 15s
          static_configs:
            - targets:
                - "kafka-controller-b-1:7071"
                - "kafka-controller-b-2:7071"
                - "kafka-controller-b-3:7071"
                - "kafka-broker-b-1:7071"
                - "kafka-broker-b-2:7071"
                - "kafka-broker-b-3:7071"
              labels: { site: "b" }
        - job_name: "kafka-mirror"
          scrape_interval: 15s
          static_configs:
            - targets: ["kafka-mirror-1:7071", "kafka-mirror-2:7071"]
```

O label `site` permite comparar os dois clusters no mesmo painel.

Kafka só expõe JMX, e o collector só raspa Prometheus — daí a imagem própria:

```dockerfile
FROM apache/kafka:4.1.1
USER root
ARG JMX_AGENT_VERSION=1.4.0
ADD https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_AGENT_VERSION}/jmx_prometheus_javaagent-${JMX_AGENT_VERSION}.jar \
    /opt/jmx/jmx_prometheus_javaagent.jar
COPY jmx/kafka.yml /opt/jmx/kafka.yml
RUN chmod -R a+r /opt/jmx
USER appuser
```

O script `launch` da imagem repassa `KAFKA_OPTS` direto para a JVM, então o
`-javaagent` funciona sem entrypoint customizado. Efeito colateral conhecido: o
wrapper também converte `KAFKA_*` em propriedades, gerando o warning
`The configuration 'opts' was supplied but isn't a known config`. Inofensivo.

Métricas que sustentam os exercícios:

| Métrica | Para quê |
|---|---|
| `kafka_server_replicamanager_underreplicatedpartitions` | sobe ao matar broker, volta a zero ao restaurar |
| `kafka_controller_kafkacontroller_activecontrollercount` | deve somar exatamente 1 por cluster |
| `kafka_server_replicamanager_isrshrinkspersec` / `isrexpandspersec` | o instante do encolhimento do ISR |
| `kafka_connect_mirror_source_connector_replication_latency_ms` | latência A→B |
| `kafka_consumer_fetch_manager_records_lag` (grupo do MM2) | o RPO com um site fora do ar |

## Impacto nas apps existentes

Nenhuma lógica de negócio muda; muda a que cluster cada app aponta e com que
nome de tópico.

| App | Site | Bootstrap | Observação |
|---|---|---|---|
| `api` (Go) | A | `localhost:19092,localhost:19093,localhost:19094` | Produz `orders.created`. O registry `platform.Topic` em `cmd/api/main.go` já isola isso — só o valor de `ORDER_INPUT_TOPIC` muda (`order.created` → `orders.created`). |
| `orders` (Streams) | A | idem | Consome `site-b.antifraud.analysis-completed` |
| `antifraud` (Streams) | B | `localhost:29092,localhost:29093,localhost:29094` | Consome `site-a.orders.fraud-requested` |

As duas apps Streams precisam de `replication.factor=3` em `StreamsConfig` para
que os tópicos internos (repartition, changelog) também nasçam replicados —
caso contrário o state store vira ponto único de falha dentro de um cluster que
é HA em todo o resto.

Escrever esses ajustes de configuração faz parte deste escopo; implementar a
lógica Streams dos domínios, não.

## Layout de arquivos

```
infra/kafka/
  Makefile
  Dockerfile
  jmx/kafka.yml
  site-a.yaml           network site-a
  site-b.yaml           network site-b
  mirror.yaml           networks [site-a, site-b]
  observability.yaml    networks [site-a, site-b]
  site-a/   common.env  controller.env  broker.env
  site-b/   common.env  controller.env  broker.env
  mirror/   mm2.properties
  observability/  otelcol-config.yaml  kafbat.yaml
  scripts/  create-topics.sh
```

As redes são criadas pelo `Makefile` (`docker network create site-a`) e
declaradas `external: true` nos quatro compose. Sem isso, `mirror.yaml` não
consegue entrar numa rede que pertence a outro projeto compose.

A imagem é construída por `make build` e referenciada por tag em todos os
compose — evita race de build entre quatro arquivos.

Alvos do `Makefile`: `build`, `up`, `topics`, `down`, `kill-broker SITE=a N=2`,
`kill-site SITE=b`.

## Verificação

Os exercícios de falha são a verificação do design. Cada um tem resultado
previsto e observável em kafbat e Grafana:

| # | Ação | Esperado |
|---|---|---|
| 1 | derruba 1 broker de A | ISR cai de 3 para 2, `underreplicatedpartitions` > 0, produção continua |
| 2 | derruba 2 brokers de A | producer com `acks=all` falha com `NOT_ENOUGH_REPLICAS` — a garantia funcionando, não um bug |
| 3 | derruba 1 controller de A | cluster segue; `kafka-metadata-quorum --status` mostra novo leader |
| 4 | derruba 2 controllers de A | metadata congela: não cria tópico, não elege líder novo; tráfego existente sobrevive um tempo |
| 5 | `make kill-site SITE=b` | orders continua produzindo, lag do MM2 sobe; ao subir B, drena. Mede o RPO |
| 6 | mata 1 instância do MM2 | a outra assume as tasks; o gap aparece no gráfico de latência |
| 7 | reenvia o mesmo `orderId` | testa a idempotência que o at-least-once do MM2 torna obrigatória |

Critério de pronto: os 7 exercícios produzem o resultado previsto, e o fluxo
completo `orders.created` → `orders.processed` atravessa os dois clusters.

## Fora de escopo

- Segurança (SASL/SCRAM, TLS, ACLs) — D8.
- Quorum KRaft dinâmico — D7, fica como exercício posterior.
- MM2 sobre Connect distribuído com API REST — evolução após os fluxos
  funcionarem.
- Exercício de failover active/passive (apontar apps para o outro site e
  traduzir offsets via `RemoteClusterUtils`).
- Containerizar as apps `orders` e `antifraud` — D6.

## Entregável seguinte

Após a implementação, documentar a **paridade com AWS MSK**: mapear cada peça do
compose para o equivalente gerenciado (MSK provisioned vs serverless, MSK
Replicator vs MM2, autenticação IAM, distribuição multi-AZ, Cruise Control,
CloudWatch/Prometheus endpoint) e registrar explicitamente o que o lab local não
consegue simular.
