# Kafka multi-cluster HA — design

Data: 2026-08-07
Status: implementado e verificado. Ver "Resultado medido" e "Desvios da implementação".

## Objetivo

Substituir a ausência de infraestrutura do `kafka-streams-lab` por **dois clusters Kafka
independentes, cada um em alta disponibilidade, replicando entre si via MirrorMaker 2**.

A motivação é didática: aprender gerenciamento de Kafka multi-cluster —
quorum KRaft, fator de replicação, `min.insync.replicas`, replicação assíncrona
entre clusters e o que quebra quando um nó cai.

O desenho não é arbitrário: ele mapeia os dois domínios que o lab já define
(`orders` e `antifraud`) para os dois clusters, de forma que a fronteira de
replicação coincida com a fronteira de domínio.

## Decisões

| # | Decisão | Alternativas descartadas |
|---|---|---|
| D1 | **Active/active por domínio**: cluster A hospeda `orders`, cluster B hospeda `antifraud`. Cada app Streams fala só com o cluster local. | Active/passive (DR) — não usa a divisão de domínios que o lab já tem. |
| D2 | **Controllers e brokers em processos separados**: 3 + 3 por cluster. | Papéis combinados (3 nós/cluster) — perde a lição de plano de controle falhando independente do plano de dados. |
| D3 | **Redes Docker isoladas** (`cluster-a`, `cluster-b`), sem rota entre elas. MM2, kafbat e o collector são as únicas travessias. | Rede única — permitiria uma app falar com o cluster errado sem erro visível. |
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
        rede docker: cluster-a                          rede docker: cluster-b
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

Os `node.id` se repetem entre os clusters de propósito — são clusters distintos,
com `CLUSTER_ID` diferente. Isso mantém os `.env` simétricos: a única diferença
entre `cluster-a/common.env` e `cluster-b/common.env` é o `CLUSTER_ID` e os hostnames
do quorum.

`CLUSTER_ID` gerados com `kafka-storage random-uuid`:

- cluster-a: `jCRZjivpSlC9UCHGEccVuw`
- cluster-b: `c4rUZKUKRzqJyLC9N6kpcw`

Os apps rodam no host (`./mvnw`, `go run`), por isso cada broker publica um
listener `EXTERNAL`: `orders` aponta para `localhost:19092,19093,19094` e
`antifraud` para `localhost:29092,29093,29094`.

### Orçamento de memória

Host: 31Gi total, ~12Gi livres. Heap capado por nó (controller 256m, broker 512m).

| Componente | Limite unitário | Total de limites |
|---|---|---|
| 6 controllers | 768M | 4,5Gi |
| 6 brokers | 1G | 6,0Gi |
| 2 MM2 | 1G | 2,0Gi |
| kafbat | 512M | 0,5Gi |
| grafana-lgtm | 2G | 2,0Gi |
| **Total** | | **~15Gi** |

Limite não é alocação: o consumo real fica em torno de heap + ~200M de overhead
da JVM por nó, ou seja ~10,5Gi com tudo no ar. Os limites são folgados de
propósito — apertá-los faz o cgroup matar a JVM, e um OOM kill se parece com um
bug do Kafka para quem está aprendendo.

Ainda assim é apertado quando as duas JVMs de Streams também estiverem rodando.
`observability.yaml` sobe como projeto separado justamente para poder ficar fora
quando não se está medindo.

## Configuração de HA

`cluster-{a,b}/common.env`:

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

# as duas únicas linhas que diferem entre cluster-a e cluster-b:
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

| Cluster | Tópico | Origem |
|---|---|---|
| A | `orders.created` | API Go / produtor externo |
| A | `orders.validated` | app `orders` |
| A | `orders.fraud-requested` | app `orders` — **replicado para B** |
| A | `orders.processed` | app `orders` |
| A | `cluster-b.antifraud.analysis-completed` | MM2, vindo de B |
| B | `cluster-a.orders.fraud-requested` | MM2, vindo de A |
| B | `antifraud.analysis-completed` | app `antifraud` — **replicado para A** |

### `infra/kafka/mirror/mm2.properties`

```properties
clusters = cluster-a, cluster-b
cluster-a.bootstrap.servers = kafka-broker-a-1:9092,kafka-broker-a-2:9092,kafka-broker-a-3:9092
cluster-b.bootstrap.servers = kafka-broker-b-1:9092,kafka-broker-b-2:9092,kafka-broker-b-3:9092

cluster-a->cluster-b.enabled = true
cluster-a->cluster-b.topics  = orders\.fraud-requested
cluster-b->cluster-a.enabled = true
cluster-b->cluster-a.topics  = antifraud\.analysis-completed

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

dedicated.mode.enable.internal.rest = true
listeners = http://0.0.0.0:8083
```

`tasks.max = 3` é o que faz as duas instâncias dividirem trabalho. Com o default
`1`, uma instância ficaria ociosa e a HA da camada de replicação não seria
observável.

As duas últimas linhas são obrigatórias para rodar **mais de uma** instância em
dedicated mode (KIP-710): os workers seguidores encaminham mudança de
configuração ao líder por esse canal REST interno. O host anunciado é o hostname
do container — por isso `mirror.yaml` define `hostname:` igual ao service name,
que é o alias de DNS registrado na rede.

### Por que o prefixo existe

`DefaultReplicationPolicy` renomeia o tópico remoto para `<cluster-origem>.<tópico>`.
Não é cosmético: **impede loop infinito**. Em replicação bidirecional com nomes
iguais, o fluxo A→B copiaria `orders.fraud-requested` para B e o fluxo B→A
copiaria de volta, indefinidamente. Com prefixo, o MM2 reconhece que
`cluster-a.orders.fraud-requested` já é remoto e não o replica de volta.

`IdentityReplicationPolicy` evita o prefixo, mas só é segura em active/passive
unidirecional.

### Consequências para as apps Streams

```java
// antifraud — consome remoto, produz local
builder.stream("cluster-a.orders.fraud-requested", ...)
       .to("antifraud.analysis-completed");

// orders — produz local, consome remoto
builder.stream("cluster-b.antifraud.analysis-completed", ...)
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
tenha 3 partições e a mesma chave. Mudar `num.partitions` em um cluster só quebra
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
        - job_name: "kafka-cluster-a"
          scrape_interval: 15s
          static_configs:
            - targets:
                - "kafka-controller-a-1:7071"
                - "kafka-controller-a-2:7071"
                - "kafka-controller-a-3:7071"
                - "kafka-broker-a-1:7071"
                - "kafka-broker-a-2:7071"
                - "kafka-broker-a-3:7071"
              labels: { cluster: "cluster-a" }
        - job_name: "kafka-cluster-b"
          scrape_interval: 15s
          static_configs:
            - targets:
                - "kafka-controller-b-1:7071"
                - "kafka-controller-b-2:7071"
                - "kafka-controller-b-3:7071"
                - "kafka-broker-b-1:7071"
                - "kafka-broker-b-2:7071"
                - "kafka-broker-b-3:7071"
              labels: { cluster: "cluster-b" }
        - job_name: "kafka-mirror"
          scrape_interval: 15s
          static_configs:
            - targets: ["kafka-mirror-1:7071", "kafka-mirror-2:7071"]
```

O label `cluster` permite comparar os dois clusters no mesmo painel.

Kafka só expõe JMX, e o collector só raspa Prometheus — daí a imagem própria:

```dockerfile
FROM apache/kafka:4.1.1
USER root
ARG JMX_AGENT_VERSION=1.0.1
ADD https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_AGENT_VERSION}/jmx_prometheus_javaagent-${JMX_AGENT_VERSION}.jar \
    /opt/jmx/jmx_prometheus_javaagent.jar
COPY jmx/kafka.yml /opt/jmx/kafka.yml
COPY with-jmx.sh /opt/jmx/with-jmx.sh
RUN chmod -R a+rX /opt/jmx && chmod a+x /opt/jmx/with-jmx.sh
USER appuser
CMD ["/opt/jmx/with-jmx.sh", "/etc/kafka/docker/run"]
```

`1.0.1` é a última versão publicada no Maven Central para este `groupId`
(confirmado em `maven-metadata.xml`); não existe `1.1+`.

**O `-javaagent` não pode viver no environment do container.** O environment é
herdado por `docker exec`, então toda ferramenta CLI (`kafka-topics.sh`,
`kafka-metadata-quorum.sh`, `kafka-broker-api-versions.sh`) carregaria o agent e
morreria tentando bindar a 7071 que o servidor já ocupa:

```
Failed to start Prometheus JMX Exporter
java.net.BindException: Address in use
```

O wrapper `with-jmx.sh` exporta `KAFKA_OPTS` e faz `exec "$@"`, então só o
processo que passa por ele recebe o agent. `mirror.yaml` também o interpõe antes
de `connect-mirror-maker.sh`.

`jmx/kafka.yml` é o exemplo canônico do repo do jmx_exporter
(tag `1.0.1`, `example_configs/kafka-2_0_0.yml`) acrescido de regras para
`kafka.connect.mirror`, `kafka.connect` e os clientes internos — sem elas o
`/metrics` dos containers do MM2 vem praticamente vazio, porque o exemplo
upstream só cobre o domínio `kafka.*` do broker.

Métricas que sustentam os exercícios:

| Métrica | Para quê |
|---|---|
| `kafka_server_replicamanager_underreplicatedpartitions` | sobe ao matar broker, volta a zero ao restaurar |
| `kafka_controller_kafkacontroller_activecontrollercount` | deve somar exatamente 1 por cluster |
| `kafka_server_replicamanager_isrshrinkspersec` / `isrexpandspersec` | o instante do encolhimento do ISR |
| `kafka_connect_mirror_source_connector_replication_latency_ms` | latência A→B |
| `kafka_consumer_fetch_manager_records_lag` (grupo do MM2) | o RPO com um cluster fora do ar |

## Impacto nas apps existentes

Nenhuma lógica de negócio muda; muda a que cluster cada app aponta e com que
nome de tópico.

| App | Cluster | Bootstrap | Observação |
|---|---|---|---|
| `api` (Go) | A | `localhost:19092,localhost:19093,localhost:19094` | Produz `orders.created`. O registry `platform.Topic` em `cmd/api/main.go` já isola isso — só o valor de `ORDER_INPUT_TOPIC` muda (`order.created` → `orders.created`). |
| `orders` (Streams) | A | idem | Consome `cluster-b.antifraud.analysis-completed` |
| `antifraud` (Streams) | B | `localhost:29092,localhost:29093,localhost:29094` | Consome `cluster-a.orders.fraud-requested` |

As duas apps Streams precisam de `replication.factor=3` em `StreamsConfig` para
que os tópicos internos (repartition, changelog) também nasçam replicados —
caso contrário o state store vira ponto único de falha dentro de um cluster que
é HA em todo o resto.

Escrever esses ajustes de configuração faz parte deste escopo; implementar a
lógica Streams dos domínios, não.

## Layout de arquivos

```
compose.yaml                   ponto de entrada — include dos quatro abaixo
Makefile
infra/kafka/
  Dockerfile
  with-jmx.sh                  injeta o javaagent só no processo do servidor
  jmx/kafka.yml
  cluster-a.yaml               network cluster-a
  cluster-b.yaml               network cluster-b
  mirror.yaml                  networks [cluster-a, cluster-b]
  observability.yaml           networks [cluster-a, cluster-b]
  cluster-a/  common.env       o que varia por cluster
  cluster-b/  common.env
  roles/      controller.env  broker.env    o que varia por papel
  mirror/     mm2.properties
  observability/  otelcol-config.yaml
  scripts/    create-topics.sh
```

`compose.yaml` na raiz faz `include` dos quatro arquivos: `docker compose up -d`
sobe tudo, e eles continuam separados para ficarem legíveis isolados — abrir
`cluster-a.yaml` mostra um cluster, não os 16 serviços intercalados.

Como `include` junta tudo num projeto só, três coisas melhoram em relação a
quatro projetos separados: o MM2 pode declarar `depends_on` nos brokers dos dois
clusters (antes ele subia antes deles e ficava em crash loop até responderem), as
redes deixam de precisar de `external: true` e são criadas e destruídas pelo
próprio compose, e `docker compose ps` mostra o lab inteiro.

O preço: derrubar um cluster inteiro deixa de ser `docker compose -f X down` e
passa a ser um `stop` com a lista de serviços daquele cluster — o `Makefile`
esconde isso em `make kill-cluster CLUSTER=b`.

`controller.env` e `broker.env` ficam em `roles/`, não duplicados dentro de cada
`cluster-*/`: eles não variam entre clusters, e duplicá-los criaria dois arquivos
que precisam ser mantidos idênticos à mão. O que varia por cluster é só o
`common.env` — e nele, só duas linhas.

kafbat é configurado por variáveis de ambiente no próprio `observability.yaml`,
sem arquivo `kafbat.yaml` à parte: são quatro variáveis para registrar dois
clusters.

As redes são criadas pelo `Makefile` (`docker network create cluster-a`) e
declaradas `external: true` nos quatro compose. Sem isso, `mirror.yaml` não
consegue entrar numa rede que pertence a outro projeto compose.

A imagem é construída por `make build` e referenciada por tag em todos os
compose — evita race de build entre quatro arquivos.

Alvos do `Makefile`: `build`, `up`, `topics`, `down`, `kill-broker CLUSTER=a N=2`,
`kill-cluster CLUSTER=b`.

## Verificação

Os exercícios de falha são a verificação do design. Cada um tem resultado
previsto e observável em kafbat e Grafana:

| # | Ação | Esperado |
|---|---|---|
| 1 | derruba 1 broker de A | ISR cai de 3 para 2, `underreplicatedpartitions` > 0, produção continua |
| 2 | derruba 2 brokers de A | producer com `acks=all` falha com `NOT_ENOUGH_REPLICAS` — a garantia funcionando, não um bug |
| 3 | derruba 1 controller de A | cluster segue; `kafka-metadata-quorum --status` mostra novo leader |
| 4 | derruba 2 controllers de A | metadata congela: não cria tópico, não elege líder novo; tráfego existente sobrevive um tempo |
| 5 | `make kill-cluster CLUSTER=b` | orders continua produzindo, lag do MM2 sobe; ao subir B, drena. Mede o RPO |
| 6 | mata 1 instância do MM2 | a outra assume as tasks; o gap aparece no gráfico de latência |
| 7 | reenvia o mesmo `orderId` | testa a idempotência que o at-least-once do MM2 torna obrigatória |

Critério de pronto: os 7 exercícios produzem o resultado previsto, e o fluxo
completo `orders.created` → `orders.processed` atravessa os dois clusters.

### Resultado medido na infraestrutura

Exercícios 1 e 2 executados após o `make up`, com `orders.fraud-requested`
(3 partições, RF=3, `min.insync.replicas=2`):

```
antes                      Partition 0  Isr: 13,11,12
                           Partition 1  Isr: 11,12,13
                           Partition 2  Isr: 12,13,11   Leader: 12

docker stop kafka-broker-a-2   (node.id 12)

depois                     Partition 0  Isr: 13,11
                           Partition 1  Isr: 11,13
                           Partition 2  Isr: 13,11      Leader: 13   <- migrou
produção com acks=all      exit 0                                    <- seguiu

docker stop kafka-broker-a-3   (segundo broker)

produção com acks=all      NOT_ENOUGH_REPLICAS / NotEnoughReplicasException

docker start dos dois      Partition 0..2  Isr: 11,12,13             <- reconstruiu
```

Replicação bidirecional verificada: duas mensagens produzidas em
`orders.fraud-requested` (cluster A) foram lidas em
`cluster-a.orders.fraud-requested` (cluster B) com a chave preservada. O MM2
criou sozinho os tópicos espelhados e os seus internos nos dois lados.

Quorum KRaft: `CurrentVoters` com 3 nós, `activecontrollercount` somando
exatamente 1 por cluster. O collector do lgtm confirma 3 brokers por cluster
respondendo em `/metrics`, e as métricas do MM2
(`kafka_connect_mirror_mirrorsourceconnector_*`) chegam com labels `topic`,
`partition` e `target`.

Exercícios 3 a 7 ficam para a sessão de estudo — a infraestrutura que eles
exigem está no ar e os alvos do `Makefile` existem.

## Desvios da implementação

O que mudou entre o design aprovado e o que está no disco, e por quê.

| # | Desvio | Motivo |
|---|---|---|
| 1 | `site-a`/`site-b` → `cluster-a`/`cluster-b` em tudo | "Site" é vocabulário de DR geográfico e o lab não simula geografia. O prefixo do MM2 vira `cluster-a.orders.fraud-requested`, que se explica sozinho. |
| 2 | `controller.env` e `broker.env` movidos para `roles/` | Eram idênticos nos dois clusters; duplicá-los criaria dois pares para manter em sincronia à mão. |
| 3 | `KAFKA_OPTS` saiu do `common.env`, entrou no wrapper `with-jmx.sh` | O environment do container é herdado por `docker exec`: toda ferramenta CLI carregava o javaagent e morria com `BindException: Address in use` na 7071 do servidor. Isso quebrava `create-topics.sh`. |
| 4 | `jmx_prometheus_javaagent` 1.4.0 → 1.0.1 | 1.4.0 não existe. 1.0.1 é a última no Maven Central para o `groupId`. |
| 5 | Healthchecks usam `["CMD","bash","-c",...]`, não `CMD-SHELL` | `/bin/sh` da imagem não é bash e não tem `/dev/tcp`: `can't create /dev/tcp/localhost/9093: nonexistent directory`. |
| 6 | `mm2.properties` ganhou `dedicated.mode.enable.internal.rest` + `listeners` | KIP-710: rodar mais de uma instância em dedicated mode exige o canal REST interno para os workers seguidores propagarem config ao líder. |
| 7 | kafbat configurado por variáveis de ambiente, sem `kafbat.yaml` | Quatro variáveis registram os dois clusters; um arquivo a menos. |
| 8 | Limites de memória subiram (controller 768M, broker 1G) | Limite apertado faz o cgroup matar a JVM, e um OOM kill se parece com bug do Kafka para quem está aprendendo. Consumo real continua ~10,5Gi. |
| 9 | `kill-cluster` usa `docker compose stop`, não `down` | `stop` preserva os volumes, então `start-cluster` traz o cluster de volta com os dados e dá para observar o MM2 drenando o lag acumulado. Com `down`, o exercício 5 perderia o estado. |
| 10 | `compose.yaml` na raiz com `include`, um projeto compose só; `Makefile` também na raiz | Pedido do usuário. Ganhou-se `depends_on` do MM2 nos brokers dos dois clusters e redes gerenciadas pelo próprio compose; perdeu-se `docker compose -f X down` por cluster, substituído por `stop` com lista de serviços. |

### Limite conhecido do isolamento de rede

O isolamento entre `cluster-a` e `cluster-b` (D3) só vale **entre containers**.
As apps rodam no host (D6), e o host alcança as duas portas:

```
localhost:19092  -> cluster-a
localhost:29092  -> cluster-b     <- nada impede
```

Se o `antifraud` apontar para `localhost:19092` por engano, ele conecta no
cluster A e o fluxo funciona *quase* — até se notar que o MM2 virou enfeite. A
rede não avisa.

O que o isolamento ainda garante: `make kill-cluster CLUSTER=b` derruba um
cluster inteiro de verdade, e o MM2 continua sendo a única ponte de **dados**
entre eles. O que se perde é a barreira contra erro de configuração das apps.

## Fora de escopo

- Segurança (SASL/SCRAM, TLS, ACLs) — D8.
- Quorum KRaft dinâmico — D7, fica como exercício posterior.
- MM2 sobre Connect distribuído com API REST — evolução após os fluxos
  funcionarem.
- Exercício de failover active/passive (apontar apps para o outro cluster e
  traduzir offsets via `RemoteClusterUtils`).
- Containerizar as apps `orders` e `antifraud` — D6.

## Entregável seguinte

Após a implementação, documentar a **paridade com AWS MSK**: mapear cada peça do
compose para o equivalente gerenciado (MSK provisioned vs serverless, MSK
Replicator vs MM2, autenticação IAM, distribuição multi-AZ, Cruise Control,
CloudWatch/Prometheus endpoint) e registrar explicitamente o que o lab local não
consegue simular.
