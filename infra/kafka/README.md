# Lab Kafka multi-cluster

Dois clusters Kafka independentes, cada um em alta disponibilidade, replicando
entre si via MirrorMaker 2. Desenho completo e justificativas em
[`docs/superpowers/specs/2026-08-07-kafka-multi-cluster-ha-design.md`](../../docs/superpowers/specs/2026-08-07-kafka-multi-cluster-ha-design.md).

```
        rede docker: cluster-a                     rede docker: cluster-b
  3 controllers (node.id 1-3)                3 controllers (node.id 1-3)
  3 brokers     (node.id 11-13)              3 brokers     (node.id 11-13)
  dominio: orders                            dominio: antifraud
        localhost:19092-19094                      localhost:29092-29094
                        \                    /
                     kafka-mirror-1, kafka-mirror-2
                     kafbat :8080   grafana :3000
```

## Subir

```bash
make up
```

Cria as redes, constroi a imagem, sobe os dois clusters, cria os topicos, sobe o
MirrorMaker 2 e a observabilidade. `make help` lista todos os alvos.

| | |
|---|---|
| kafbat | http://localhost:8080 |
| Grafana | http://localhost:3000 |
| cluster-a | `localhost:19092,localhost:19093,localhost:19094` |
| cluster-b | `localhost:29092,localhost:29093,localhost:29094` |

## Topicos

| Cluster | Topico | Quem cria |
|---|---|---|
| A | `orders.created`, `orders.validated`, `orders.fraud-requested`, `orders.processed` | `scripts/create-topics.sh` |
| A | `cluster-b.antifraud.analysis-completed` | MirrorMaker 2 |
| B | `antifraud.analysis-completed` | `scripts/create-topics.sh` |
| B | `cluster-a.orders.fraud-requested` | MirrorMaker 2 |

O prefixo `cluster-a.` **e o nome real do topico dentro do cluster B** -- nao e
endereco nem namespace de rede. Quem consome do outro lado pede o nome
prefixado; `orders.fraud-requested` simplesmente nao existe no cluster B.

## Exercicios de falha

```bash
make kill-broker CLUSTER=a N=2      # 1 broker fora: ISR cai para 2, producao segue
make kill-broker CLUSTER=a N=3      # 2 brokers fora: acks=all falha, NOT_ENOUGH_REPLICAS
make start-broker CLUSTER=a N=2

make kill-controller CLUSTER=a N=1  # plano de controle tolera perder 1 de 3
make start-controller CLUSTER=a N=1

make kill-cluster CLUSTER=b         # cluster inteiro fora: lag do MM2 = o RPO
make start-cluster CLUSTER=b

make quorum CLUSTER=a               # estado do quorum KRaft
make describe CLUSTER=a             # ISR de todos os topicos
make ps
```

`kill-*` usa `docker stop`, nao `down`: os volumes ficam, entao da para observar
o ISR sendo reconstruido e o MM2 drenando o lag acumulado ao restaurar.

## Detalhes que costumam surpreender

**Ferramentas CLI e o javaagent.** O `-javaagent` do Prometheus e injetado por
`with-jmx.sh`, nao pelo environment do container. Environment e herdado por
`docker exec`, e toda ferramenta CLI morreria com `BindException: Address in use`
tentando bindar a 7071 que o servidor ja ocupa.

**Isolamento vale entre containers, nao a partir do host.** As apps rodam no
host e alcancam `localhost:19092` e `localhost:29092`. Se uma app apontar para o
cluster errado, ela conecta e funciona quase -- a rede nao avisa. O isolamento
garante que o MM2 seja a unica ponte de dados, nao que voce nao erre a config.

**EOS nao atravessa cluster.** O MM2 e um consumer comum lendo de um cluster e um
producer comum escrevendo no outro: replicacao at-least-once, duplicatas cruzam a
fronteira. E por isso que a regra de idempotencia por `orderId` e obrigatoria.

## Limpar

```bash
make down     # para tudo, preserva volumes
make clean    # apaga volumes e redes tambem
```
