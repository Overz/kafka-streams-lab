# Kafka multi-cluster HA — design

Data: 2026-08-07
Atualizado: 2026-08-16 — a camada de observabilidade e o SQL sobre tópicos foram
trocados; o desenho dos clusters não mudou. Ver desvios 11 a 14.
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
| D3 | **Redes Docker isoladas** (`cluster-a`, `cluster-b`), sem rota entre elas. As travessias são MM2, kafbat, otelcol e Flink — só o MM2 carrega dado de aplicação. | Rede única — permitiria uma app falar com o cluster errado sem erro visível. |
| D4 | **MM2 em dedicated mode, 2 instâncias**. | Connect distribuído + REST (mais artefatos antes do primeiro byte replicado); dois processos unidirecionais (nenhum HA sozinho). |
| D5 | **Observabilidade**: kafbat (estado atual) + otelcol → OpenObserve (histórico). | `grafana/otel-lgtm` — foi o que rodou até 2026-08-16 (desvio 11); Prometheus + Grafana separados — mais containers para o mesmo resultado. |
| D6 | **Apps rodam no host**, não no compose. | Containerizar as apps — atrapalha o ciclo de reescrever código Streams. |
| D7 | **Quorum KRaft estático** (`controller.quorum.voters`). | Quorum dinâmico do 4.x — melhor exercício, fica para depois; o estático deixa o quorum legível num arquivo enquanto se aprende a forma. |
| D8 | **Sem segurança no Kafka** (PLAINTEXT em tudo). O login exigido é o do kafbat (D9), não dos brokers. | SASL/SCRAM + TLS — muito tempo depurando config antes de aprender o que interessa. Fica fora do escopo. |
| D9 | **Login único pelo Dex** (IdP local) para kafbat e OpenObserve; kafbat com OAuth2 e MCP ligados, config em arquivo estático + dynamic config. | Sem autenticação — deixaria metade das features do kafbat fora do lab; provedor externo (Google/GitHub) — client secret de verdade e dependência de internet num lab offline; dois logins separados — o lab passaria a ensinar o contrário do que se faz em produção. |
| D10 | **SQL contínuo sobre os tópicos com Flink**, imagem própria com o `flink-sql-connector-kafka`. | ksqlDB — só existe em imagem `confluentinc`, sob Confluent Community License, e obrigaria o lab a depender de um binário não-OSI para uma feature acessória. |

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
                 mirror-1, mirror-2      kafbat :8080  + dex :5556
                 (MM2 dedicated)         otelcol -> openobserve :5080
                 nas duas redes          flink :8081
                                         nas duas redes
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
| kafbat | 768M | 0,75Gi |
| dex | 192M | 0,19Gi |
| openobserve | 2G | 2,0Gi |
| otelcol | 512M | 0,5Gi |
| flink-jobmanager | 1200M | 1,17Gi |
| flink-taskmanager | 1800M | 1,76Gi |
| **Total** | | **~19Gi** |

Limite não é alocação: o consumo real medido com os 20 containers de longa duracao no ar ficou em
**~7,6Gi**. Os limites são folgados de propósito — apertá-los faz o cgroup matar
a JVM, e um OOM kill se parece com um bug do Kafka para quem está aprendendo.

Ainda assim é apertado quando as duas JVMs de Streams também estiverem rodando.
Daí os alvos que desligam por camada: `make observability-down` (kafbat, dex,
openobserve, otelcol) e `make flink-down` (~0,9Gi reais).

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

### `infra/mirrormaker/mm2.properties`

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
do container — por isso `mirrormaker/compose.yaml` define `hostname:` igual ao service name,
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

Quatro peças, todas nas duas redes.

**kafbat** com os dois clusters registrados — estado atual: ISR por partição,
partições under-replicated, controller ativo, tópicos com prefixo aparecendo do
outro lado, lag do consumer group do MM2. Configurado por arquivo estático
(`kafbat/config.yaml`, montado read-only) somado ao dynamic config
que o wizard grava em `/etc/kafkaui/dynamic_config.yaml`. Métricas via
`type: PROMETHEUS` na 7071 — o mesmo endpoint que o collector raspa, sem abrir
JMX/RMI para o kafbat.

**dex** — IdP OIDC local, um login para os dois consoles. O detalhe que quebra em
docker é o browser e o container enxergarem o IdP por nomes diferentes, e cada
cliente resolve isso de um jeito:

- **kafbat** aceita endpoints separados. Não usamos `issuer-uri` (dispararia
  discovery e fixaria um host só): `authorization-uri` aponta para
  `localhost:5556` (browser) e `token`/`jwks`/`userinfo` para `dex:5556`
  (container).
- **OpenObserve** tem uma URL só (`O2_DEX_BASE_URL`) para as duas pontas. A saída
  foi rodar o Dex **dentro do namespace de rede do OpenObserve**
  (`network_mode: service:openobserve`), onde `localhost:5556` vale para o
  browser e para o processo; um alias de rede mantém o nome `dex` resolvível
  para o kafbat.

Três preços dessa montagem, os três verificados na prática:

1. Restart ou recriação do OpenObserve refaz o sandbox de rede e deixa o Dex num
   namespace órfão — a 5556 some do host até reiniciá-lo. Daí
   `make openobserve-restart`, que reinicia os três na ordem.
2. O storage do Dex precisa ser sqlite em volume, não memória: as chaves de
   assinatura vivem ali, e chaves novas a cada restart fazem o OpenObserve
   responder 401 enquanto o cache de JWKS dele (10 min) não expira.
3. O kafbat guarda conexões para o Dex num pool que, por padrão, não expira.
   Depois de um restart do Dex a conexão guardada está morta e o POST de troca de
   token não pode ser repetido (o corpo já foi enviado): o login morre com
   `Connection reset by peer` e 500 em `/login/oauth2/code/dex`. Resolvido com
   `-Dreactor.netty.pool.maxIdleTime=5000` no `JAVA_OPTS`.

**otelcol** (distribuição contrib — o receiver `prometheus` não existe na core) —
raspa 14 targets, lê o stdout dos 14 containers do Kafka e exporta tudo por
OTLP/HTTP. Métrica e log seguem caminhos diferentes de propósito: o Kafka expõe
métrica por HTTP (via javaagent) mas não expõe log por rede nenhuma, então a
única fonte de log é o arquivo que o docker escreve em
`/var/lib/docker/containers/<id>/<id>-json.log`. Como o caminho é por id, o
filtro de "quais containers" não cabe no glob: os serviços pedem ao driver
`json-file` para gravar o label `com.docker.compose.service` em cada linha, e o
receiver descarta o que vier sem ele. Daí o collector rodar como `root` com o
diretório montado read-only — a alternativa, montar o `docker.sock`, daria
controle total do host ao collector.

**openobserve** — histórico temporal: logs, métricas e traces num binário só,
consultáveis por PromQL e SQL. Não raspa nada sozinho; quem raspa é o otelcol.
Build **enterprise** (`public.ecr.aws/zinclabs/openobserve-enterprise`), porque
SSO não existe na open source — licença comercial source-available, gratuita até
50 GB/dia. Duas validações derrubam o processo no boot, não na primeira request:
e-mail do root precisa de TLD e a senha precisa de 8+ caracteres com maiúscula,
minúscula, dígito e especial. A ingestão do otelcol continua usando o root em
`Basic` auth: token de browser não serve para processo.

Os scrape jobs:

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

O destino é um exporter só para os três sinais:

```yaml
exporters:
  otlp_http/openobserve:
    endpoint: "http://openobserve:5080/api/default"   # base, sem barra final
    headers:
      Authorization: "${env:O2_AUTH}"                 # Basic base64(email:senha)
      stream-name: "kafka-lab"
```

O `endpoint` é a base: o collector acrescenta `/v1/metrics`, `/v1/traces` e
`/v1/logs` sozinho, e barra sobrando vira 404. O `stream-name` nomeia o stream de
logs e traces; métrica ignora o header, porque o nome da métrica já vira o nome do
stream.

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
processo que passa por ele recebe o agent. `mirrormaker/compose.yaml` também o interpõe antes
de `connect-mirror-maker.sh`.

`jmx/kafka.yml` é o exemplo canônico do repo do jmx_exporter
(tag `1.0.1`, `example_configs/kafka-2_0_0.yml`) acrescido de regras para
`kafka.connect.mirror`, `kafka.connect` e os clientes internos — sem elas o
`/metrics` dos containers do MM2 vem praticamente vazio, porque o exemplo
upstream só cobre o domínio `kafka.*` do broker.

Acrescido também de duas regras de `BrokerTopicMetrics ... FifteenMinuteRate` com
o nome da métrica JMX vindo como **label** (`name="BytesInPerSec"`): é a forma
que o kafbat procura para preencher as colunas de throughput. O resto do arquivo
só exporta o atributo `Count`, que é contador acumulado, e nenhuma regra casava
com `<>FifteenMinuteRate` — o efeito era throughput `null` na UI (desvio 14).

O `/metrics` na 7071 é agnóstico de backend: trocar quem consome (era o collector
embutido no lgtm, hoje o otelcol) não muda nada na imagem do Kafka.

Métricas que sustentam os exercícios:

| Métrica | Para quê |
|---|---|
| `kafka_server_replicamanager_underreplicatedpartitions` | sobe ao matar broker, volta a zero ao restaurar |
| `kafka_controller_kafkacontroller_activecontrollercount` | deve somar exatamente 1 por cluster |
| `kafka_server_replicamanager_isrshrinkspersec` / `isrexpandspersec` | o instante do encolhimento do ISR |
| `kafka_connect_mirror_source_connector_replication_latency_ms` | latência A→B |
| `kafka_consumer_fetch_manager_records_lag` (grupo do MM2) | o RPO com um cluster fora do ar |
| `kafka_server_brokertopicmetrics_fifteenminuterate{name="BytesInPerSec"}` | throughput por broker e por tópico, é o que o kafbat lê |

## SQL sobre os tópicos

`flink/compose.yaml`: session cluster com 1 jobmanager e 1 taskmanager (2 slots). O SQL
client roda dentro do jobmanager e submete pela REST na 8081 — por isso não há
container de SQL gateway.

A imagem oficial do Flink não traz conector nenhum: `/opt/flink/lib` tem só o
core, os formatos (json, csv) e o planner. `flink/Dockerfile` acrescenta o
`flink-sql-connector-kafka`; sem ele, `CREATE TABLE ... WITH ('connector'='kafka')`
falha com `Could not find any factory for identifier 'kafka'`. O par de versões é
rígido: o sufixo do conector **é** a minor do Flink.

O Flink entra nas duas redes, e isso é diferente do MM2 (D3): ele não replica
nada, lê e calcula. Um job que lê `orders.created` de A e
`antifraud.analysis-completed` de B faz um join que **não** passa pela fronteira
do MM2 — vale comparar com o caminho replicado, que tem lag e é at-least-once.

`infra/flink/sql/orders.sql` usa `'scan.bounded.mode' = 'latest-offset'` só para
terminar sozinho em `make flink-example`. O caso normal do Flink é a query ficar
de pé para sempre; no client interativo, omitir essa linha.

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
docker-compose.yaml            ponto de entrada — include dos cinco abaixo
                               + o job kafka-init-topics
Makefile                       só atalho; nada depende dele
infra/
  README.md                    como usar e exercícios de falha
  kafka/
    cluster-a.yaml             network cluster-a
    cluster-b.yaml             network cluster-b
    topics.yaml                job kafka-init-topics
    Dockerfile                 kafka + jmx_prometheus_javaagent
    with-jmx.sh                injeta o javaagent só no processo do servidor
    jmx/kafka.yml
    cluster-a/  common.env     o que varia por cluster
    cluster-b/  common.env
    roles/      controller.env  broker.env    o que varia por papel
    scripts/    create-topics.sh  roda DENTRO do container, não no host
  mirrormaker/  compose.yaml  mm2.properties
  kafbat/       compose.yaml  config.yaml
  dex/          compose.yaml  config.yaml
  openobserve/  compose.yaml
  otelcol/      compose.yaml  config.yaml
  flink/        compose.yaml  Dockerfile  sql/orders.sql
```

Uma pasta por domínio, com os arquivos que **só** aquele domínio usa. Antes tudo
morava em `infra/kafka/`, inclusive o que não é Kafka: o kafbat, o Dex e o
collector estavam sob `observability/`, e `mm2.properties` sob `mirror/`
(desvio 18).

Duas dependências atravessam pasta, ambas comentadas no ponto onde acontecem: o
MirrorMaker constrói a imagem do Kafka (`context: ../kafka`, porque o
`connect-mirror-maker.sh` vem nela) e o Dex roda no namespace de rede do
OpenObserve.

`docker-compose.yaml` na raiz faz `include` dos cinco arquivos: `docker compose up -d`
sobe tudo, e eles continuam separados para ficarem legíveis isolados — abrir
`cluster-a.yaml` mostra um cluster, não os 21 serviços intercalados.

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

kafbat é configurado por arquivo (`kafbat/config.yaml`), não por
variáveis de ambiente: registrar dois clusters cabia em quatro variáveis, mas
`auth.oauth2`, `audit` e `masking` são aninhados demais para virar
`KAFKA_CLUSTERS_0_...` sem ficar ilegível. No `kafbat/compose.yaml` sobram duas
variáveis: `DYNAMIC_CONFIG_ENABLED` e o caminho do arquivo estático.

As redes são criadas e destruídas pelo próprio compose, com `name:` fixo para não
levarem o prefixo do projeto (era `external: true` + `docker network create` no
`Makefile`, antes do `include` — desvio 10).

As imagens do lab (Kafka com javaagent, Flink com conector) trazem `build` junto
da tag nos próprios serviços, não num passo separado: `docker compose up -d`
constrói o que falta antes de criar qualquer container. A repetição do mesmo
`build` entre os serviços da imagem do Kafka é intencional — bate no cache e não
custa nada, enquanto declarar em um serviço só quebraria quem sobe um subconjunto
(`docker compose up kafka-broker-b-1`). Era `make build` com `docker build` na
mão, o que fazia o fluxo depender do Makefile (desvio 16).

Pelo mesmo motivo os tópicos viraram o job **`kafka-init-topics`**, declarado no
compose da raiz com `depends_on: service_healthy` nos seis brokers: sobe, cria e
sai com `Exited (0)`. Antes era uma linha do `make up` chamando
`create-topics.sh` do host via `docker exec`; quem usasse `docker compose up -d`
direto ficava sem tópico nenhum, e como `auto.create.topics.enable=false`, o
primeiro producer travava em `UNKNOWN_TOPIC_OR_PARTITION`.

Alvos do `Makefile` (todos são atalho de uma linha de `docker compose`): `build`,
`up`, `topics`, `down`, `clean`, `kill-broker CLUSTER=a N=2`,
`kill-cluster CLUSTER=b`, `observability-up/down`, `openobserve-restart`,
`flink-sql`, `flink-example`, `flink-up/down`.

## Verificação

Os exercícios de falha são a verificação do design. Cada um tem resultado
previsto e observável em kafbat e OpenObserve:

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
exatamente 1 por cluster. O otelcol confirma 3 brokers por cluster
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
| 7 | kafbat configurado por variáveis de ambiente, sem `kafbat.yaml` | Quatro variáveis registravam os dois clusters; um arquivo a menos. **Revertido em 2026-08-16** — ver desvio 12. |
| 8 | Limites de memória subiram (controller 768M, broker 1G) | Limite apertado faz o cgroup matar a JVM, e um OOM kill se parece com bug do Kafka para quem está aprendendo. Consumo real continua ~10,5Gi. |
| 9 | `kill-cluster` usa `docker compose stop`, não `down` | `stop` preserva os volumes, então `start-cluster` traz o cluster de volta com os dados e dá para observar o MM2 drenando o lag acumulado. Com `down`, o exercício 5 perderia o estado. |
| 10 | `docker-compose.yaml` na raiz com `include`, um projeto compose só; `Makefile` também na raiz | Pedido do usuário. Ganhou-se `depends_on` do MM2 nos brokers dos dois clusters e redes gerenciadas pelo próprio compose; perdeu-se `docker compose -f X down` por cluster, substituído por `stop` com lista de serviços. |

Mudanças de 2026-08-16, todas na camada de cima — nenhuma toca o desenho dos
clusters:

| # | Desvio | Motivo |
|---|---|---|
| 11 | `grafana/otel-lgtm` → `openobserve` + `otelcol` explícito | Um binário só para logs, métricas e traces, com PromQL **e** SQL. O collector deixa de ser detalhe escondido dentro da imagem do lgtm e vira serviço com config versionada; precisa ser a distribuição **contrib**, porque o receiver `prometheus` não existe na core. Some a pipeline de profiles: o lgtm tinha Pyroscope embutido, o OpenObserve não ingere pprof. |
| 12 | kafbat volta a ter arquivo de config (`kafbat/config.yaml`) + dynamic config em volume | Ligar OAuth2, auditoria, MCP e máscaras por variável de ambiente fica ilegível. O arquivo é read-only; o que a UI grava (wizard, uploads) vai para `kafbat-etc:/etc/kafkaui`, fora do git. |
| 13 | ksqlDB descartado, Flink no lugar | ksqlDB só existe em imagem `confluentinc` sob Confluent Community License. O preço da troca é a aba "KSQL DB" do kafbat, que fala a REST API do ksqlDB e só ela — por isso `ksqldb-server` fica comentado no config. |
| 14 | Duas regras novas em `jmx/kafka.yml` (`BrokerTopicMetrics ... FifteenMinuteRate`) | Sem elas o kafbat mostrava throughput `null`: ele procura taxa de 15 minutos com o nome da métrica JMX como label, e o arquivo só exportava o atributo `Count`. Efeito colateral bem-vindo: as mesmas séries passam a existir no OpenObserve. |
| 15 | OpenObserve open source → build enterprise, com SSO pelo mesmo Dex | Um login para os dois consoles em vez de dois. Custo: sai do AGPL para licença comercial source-available (gratuita até 50 GB/dia) e o Dex passa a rodar no namespace de rede do OpenObserve, porque o produto tem uma URL só para o IdP. |
| 16 | `build` nos serviços e job `kafka-init-topics` no compose; `Makefile` vira só atalho | O fluxo real é `docker compose up -d` / `down` / `down -v`, sem Makefile. Antes disso, quem não usasse `make` subia o lab sem a imagem construída e sem tópico nenhum. |
| 17 | Receiver `filelog` no otelcol + label `com.docker.compose.service` no driver de log dos serviços do Kafka | O OpenObserve recebia só métrica; a aba Logs ficava vazia e parecia que nada chegava. O Kafka não fala OTLP nem expõe log por rede — a única fonte é o arquivo do docker. Custo: o collector passa a rodar como `root` com `/var/lib/docker/containers` montado read-only. |
| 18 | `infra/` reorganizado por domínio: uma pasta por serviço, cada uma com o compose e os arquivos que só ela usa | `infra/kafka/` tinha virado guarda-chuva — kafbat, Dex, OpenObserve, collector e Flink moravam lá dentro, alguns aninhados em `observability/`. Achar a config de um serviço passava por saber a história do repositório. O `include` da raiz vira o índice: um arquivo por domínio, nove no total. |

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
