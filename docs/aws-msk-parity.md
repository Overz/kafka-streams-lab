# Do lab local para o AWS MSK

O que cada peça do lab vira em MSK, o que a AWS assume no seu lugar, e o que este
lab **não** consegue ensinar. Complementa
[`superpowers/specs/2026-08-07-kafka-multi-cluster-ha-design.md`](superpowers/specs/2026-08-07-kafka-multi-cluster-ha-design.md).

## A mudança de forma, em uma frase

Em MSK você não opera nós — você opera **um cluster**. Metade do que este lab
constrói (controllers, quorum KRaft, volumes por nó, listeners, formatação de
storage) simplesmente deixa de existir como coisa sua. O que sobra, e continua
sendo seu, é exatamente a parte que decide se você perde dados: fator de
replicação, `min.insync.replicas`, `acks`, eleição suja de líder, e o desenho da
replicação entre clusters.

## Mapeamento peça a peça

| No lab | Em MSK | Comentário |
|---|---|---|
| 3 containers controller por cluster | **não existe** | O plano de controle KRaft é responsabilidade da AWS. Você não vê, não configura e não derruba os controllers. |
| `CLUSTER_ID`, `kafka-storage format` | **não existe** | Provisionamento cuida disso. |
| 3 containers broker por cluster | Número de brokers do cluster provisionado | Precisa ser múltiplo do número de AZs. 3 AZs → 3, 6, 9 brokers. |
| Redes Docker isoladas `cluster-a` / `cluster-b` | Dois clusters MSK, em VPCs e/ou Regiões diferentes | O isolamento vira real: grupos de segurança, sub-redes privadas, e latência de verdade entre Regiões. |
| Brokers em containers no mesmo host | Brokers distribuídos em **Availability Zones** | `broker.rack` é definido por AZ automaticamente, e a atribuição de réplicas passa a ser rack-aware: com RF=3 em 3 AZs, perder uma AZ inteira deixa 2 réplicas de pé. É o análogo fiel do exercício 1. |
| Volume nomeado por nó | Volume EBS por broker, com auto-scaling de storage | O defeito do compose de referência (um volume para todos os nós) não tem como acontecer. |
| `common.env` com `KAFKA_*` | **MSK Configuration** — um `server.properties` versionado, anexado ao cluster | Propriedades que descrevem identidade e topologia do nó (`broker.id`, `listeners`, `advertised.listeners`, `log.dirs`, `process.roles`) não são configuráveis: são a AWS. |
| `min.insync.replicas=2`, `default.replication.factor=3`, `unclean.leader.election.enable=false` | **Idênticos, e são seus** | Vão na MSK Configuration com o mesmo nome e a mesma semântica. É a parte que transfere 1:1. |
| `EXTERNAL://localhost:19092` | Bootstrap brokers, um endereço por tipo de autenticação | `GetBootstrapBrokers` devolve strings diferentes para TLS, SASL/SCRAM e IAM. |
| 2 containers MirrorMaker 2 | **MSK Replicator** | Ver seção abaixo — a diferença mais interessante. |
| `jmx_prometheus_javaagent` + `with-jmx.sh` | **Open Monitoring**, já embutido | Expõe endpoints do JMX Exporter e do Node Exporter sem você montar nada. Os nomes das métricas JMX são os mesmos. |
| openobserve + otelcol | Amazon Managed Prometheus + Managed Grafana, ou o próprio OpenObserve lendo o endpoint | O `otelcol-config.yaml` deste lab continua válido: troca os 14 alvos estáticos por service discovery e o exporter de destino. |
| kafbat + dex | Console do MSK + CloudWatch | Ou o próprio kafbat apontado para os bootstrap brokers, com o OAuth2 trocado do Dex para Cognito/IAM Identity Center. |
| flink-jobmanager + flink-taskmanager | **Amazon Managed Service for Apache Flink** | O SQL não muda; muda quem opera o cluster. O conector Kafka vira responsabilidade do pacote de aplicação que você sobe, e o `bootstrap.servers` passa a apontar para os bootstrap brokers do MSK. |
| `make kill-broker` | **não existe como operação sua** | Substituição de broker é da AWS. Você observa o efeito, não provoca. |
| PLAINTEXT em tudo | IAM, SASL/SCRAM (Secrets Manager), mTLS (ACM Private CA) ou não-autenticado; TLS em trânsito, KMS em repouso | Ver seção de autenticação. |

## MSK Replicator vs. MirrorMaker 2

MSK Replicator é a versão gerenciada do problema que o `infra/mirrormaker/` resolve:
replicação assíncrona de dados, configurações de tópico, ACLs e offsets de
consumer group entre clusters, com escala automática e sem infraestrutura sua.

O ponto que vale entender bem, porque toca direto na decisão que este lab tomou:

**Os dois modos de nomear tópico existem lá também** — e a AWS chama de
*Prefixed* (padrão) e *Identical*.

| | Prefixed (padrão) | Identical |
|---|---|---|
| Nome no destino | `<sourceKafkaClusterAlias>.topic` | `topic` |
| Como evita ciclo | pelo próprio prefixo, igual à `DefaultReplicationPolicy` do MM2 | por **header no registro** (`__mskmr`), com identificadores de cluster e tópico de origem |
| A AWS recomenda para | active-active | active-passive, failover sem reconfigurar cliente, agregação, migração |

Isso responde a pergunta "o prefixo é necessário?" de um jeito que o MM2 puro não
responde: **a AWS resolveu o problema do ciclo sem depender do nome.** No MM2 em
dedicated mode, a proteção contra ciclo vem do `topicSource()` da replication
policy — com `IdentityReplicationPolicy` esse método devolve `null` e a proteção
some. A AWS trocou isso por metadado no registro, que é mais robusto e não
sequestra o nome do tópico.

Detalhe operacional que a doc da AWS levanta e vale trazer para o lab: com
Prefixed, consumidores que precisam sobreviver a failover devem assinar por
**regex** (`.*orders\.fraud-requested`) em vez de nome literal, para ler tanto o
tópico local quanto o replicado. Isso é diretamente aplicável ao `antifraud`
deste lab.

Outras diferenças:

- MSK Replicator suporta cross-region (CRR) e same-region (SRR); os dois clusters
  MSK precisam estar na **mesma conta AWS**.
- Replica ACLs e configurações de tópico, não só dados.
- Escala sozinho — não existe o equivalente a `tasks.max = 3` para você ajustar.
- Não existe `make kill-mirror`: o exercício 6 deste lab não tem contrapartida.

## Autenticação — o degrau que o lab pula

O lab é PLAINTEXT em tudo (decisão D8 da spec). Em MSK isso é a maior mudança de
código do cliente:

```properties
# lab
bootstrap.servers=localhost:19092,localhost:19093,localhost:19094

# MSK com IAM
bootstrap.servers=b-1.meucluster.xxxx.c2.kafka.us-east-1.amazonaws.com:9098,...
security.protocol=SASL_SSL
sasl.mechanism=AWS_MSK_IAM
sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;
sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler
```

Exige a dependência `software.amazon.msk:aws-msk-iam-auth` no `orders`, no
`antifraud` e um equivalente no cliente Go da `api`. A autorização deixa de ser
"todo mundo pode tudo" e passa a ser política IAM por tópico e por ação.

## Observabilidade

Open Monitoring expõe métricas no formato Prometheus a partir do JMX Exporter e
do Node Exporter, sem você instalar agent — o que torna o `Dockerfile` e o
`with-jmx.sh` deste lab desnecessários. **Os nomes das métricas JMX são os
mesmos**, então as regras de `jmx/kafka.yml` e qualquer painel construído aqui
continuam servindo.

Em paralelo, o CloudWatch entrega métricas agregadas em quatro níveis (`DEFAULT`,
`PER_BROKER`, `PER_TOPIC_PER_BROKER`, `PER_TOPIC_PER_PARTITION`), com nomes
próprios. As que correspondem aos exercícios deste lab:

| Exercício | Métrica JMX (lab) | Métrica CloudWatch (MSK) |
|---|---|---|
| 1 — 1 broker fora | `kafka_server_replicamanager_underreplicatedpartitions` | `UnderReplicatedPartitions` |
| 2 — 2 brokers fora | falha `NOT_ENOUGH_REPLICAS` no producer | `UnderMinIsrPartitionCount` |
| 3/4 — controllers | `kafka_controller_kafkacontroller_activecontrollercount` | `ActiveControllerCount` (existe, mas você não provoca a falha) |
| — | partições sem líder | `OfflinePartitionsCount` |
| 5 — cluster fora | lag do consumer group do MM2 | `ReplicatorBytesInPerSec`, `ReplicatorThroughput`, `MaxOffsetLag` |

Só o nível `DEFAULT` é gratuito; os outros três são cobrados por métrica.

## O que este lab ensina que transfere sem tradução

- **`min.insync.replicas` + `acks=all`.** Mesma semântica, mesmo erro
  (`NOT_ENOUGH_REPLICAS`), mesma decisão de trade-off. O exercício 2 é
  literalmente o que acontece em MSK quando uma AZ cai e você configurou RF=3
  com min.isr=2 em 3 AZs... e cai a segunda.
- **Comportamento do ISR ao perder um broker.** Idêntico. Em MSK a causa é
  substituição de instância ou falha de AZ em vez de `docker stop`.
- **`unclean.leader.election.enable=false`.** Mesma propriedade, mesmo dilema
  entre disponibilidade e não perder mensagem confirmada.
- **Prefixo de tópico em replicação e prevenção de ciclo.** Mesma escolha, com a
  variante do header que a AWS acrescenta.
- **At-least-once atravessando clusters.** MSK Replicator é assíncrono como o
  MM2: EOS não atravessa cluster lá também. A regra de idempotência por `orderId`
  continua obrigatória.
- **Lag de replicação = RPO.** Mesma conta, agora com latência de Região no meio.
- **Nomes de métrica JMX.** Iguais.

## O que este lab não consegue simular

- **Falha de AZ.** Containers no mesmo host não têm domínio de falha
  independente. `broker.rack` e atribuição rack-aware de réplicas — o mecanismo
  que faz RF=3 sobreviver a uma AZ inteira — não têm como ser exercitados aqui.
- **Latência de rede real.** Os dois clusters conversam por loopback. O lag do
  MM2 aqui mede o tempo de processamento, não distância geográfica. Sem injetar
  latência (`tc netem`), a intuição de RPO cross-region não se forma.
- **Falha de controller.** Os exercícios 3 e 4 ensinam algo que em MSK você nunca
  vai operar. Continuam valendo como entendimento do KRaft, não como prática.
- **Upgrade rolling sem downtime, substituição de broker, storage auto-scaling.**
  São operações da AWS.
- **Autorização.** Política IAM por tópico não tem análogo em PLAINTEXT.
- **Custo.** A restrição de projeto mais concreta em MSK — transferência entre
  AZs, transferência entre Regiões na replicação, métricas do CloudWatch por
  nível — não tem representação nenhuma num lab local.
- **Quotas e throttling sob carga.** Precisa de volume que este lab não gera.

## O que mudaria no código deste repositório

Nada de lógica de negócio. Só configuração:

| Arquivo | Mudança |
|---|---|
| `api/internal/config/config.go` | `KAFKA_BROKERS` passa a ser a string de bootstrap do MSK; acrescenta `security.protocol` e as propriedades SASL |
| `api/internal/platform/kafka.go` | `kafka-go` precisa do mecanismo SASL do MSK IAM (ou SASL/SCRAM, que o `kafka-go` já suporta nativamente e é o caminho mais curto) |
| `orders`, `antifraud` (`StreamsConfig`) | mesmas propriedades SASL; `replication.factor=3` continua necessário para os tópicos internos |
| `antifraud` | se o Replicator ficar em modo Prefixed, assinar por regex em vez de nome literal, para o failover não exigir redeploy |
| `infra/kafka/` e `infra/mirrormaker/` | viram Terraform: `aws_msk_cluster`, `aws_msk_configuration`, `aws_msk_replicator`. `Dockerfile` e `with-jmx.sh` deixam de existir. |

O caminho de menor atrito para experimentar em MSK sem reescrever cliente é
**SASL/SCRAM**: o `kafka-go` e o cliente Java suportam nativamente, sem
dependência extra. IAM é o que se usaria em produção, mas acrescenta um jar e um
callback handler ao ciclo.
