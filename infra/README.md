# Lab Kafka multi-cluster

Dois clusters Kafka independentes, cada um em alta disponibilidade, replicando entre si via MirrorMaker 2. Desenho
completo e justificativas em
[
`docs/superpowers/specs/2026-08-07-kafka-multi-cluster-ha-design.md`](../../docs/superpowers/specs/2026-08-07-kafka-multi-cluster-ha-design.md).

```
        rede docker: cluster-a                     rede docker: cluster-b
  3 controllers (node.id 1-3)                3 controllers (node.id 1-3)
  3 brokers     (node.id 11-13)              3 brokers     (node.id 11-13)
  dominio: orders                            dominio: antifraud
        localhost:19092-19094                      localhost:29092-29094
                        \                    /
                     kafka-mirror-1, kafka-mirror-2
             kafbat :8080  openobserve :5080  otelcol :4317/:4318
             dex :5556     flink :8081 (le os dois clusters)
```

## Subir

Todos os comandos rodam **da raiz do repositorio**.

```bash
docker compose up -d --wait     # sobe tudo: constroi as imagens e cria os topicos
docker compose down             # para tudo, preserva os volumes
docker compose down -v          # apaga volumes e redes tambem
```

Nao ha passo anterior: o compose constroi a imagem do Kafka (`build` junto da tag nos servicos) e a do Flink, cria as
redes `cluster-a` e `cluster-b`, e o servico `kafka-init-topics` cria os topicos assim que os seis brokers ficam
healthy. Ele e um job -- sobe, cria e sai com `Exited (0)`, o que e o esperado no `docker compose ps -a`.

O `Makefile` e so atalho para essas mesmas linhas (`make help` lista tudo); nada no lab depende dele.

| tarefa | docker compose | make |
|---|---|---|
| subir | `docker compose up -d --wait` | `make up` |
| derrubar | `docker compose down` | `make down` |
| limpar volumes | `docker compose down -v` | `make clean` |
| recriar topicos | `docker compose up kafka-init-topics` | `make topics` |
| derrubar 1 broker | `docker compose stop kafka-broker-a-2` | `make kill-broker CLUSTER=a N=2` |
| liberar RAM | `docker compose stop kafbat dex openobserve otelcol` | `make observability-down` |
| SQL do Flink | `docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh` | `make flink-sql` |
| reiniciar o SSO | `docker compose restart openobserve && docker compose restart dex && docker compose restart kafbat` | `make openobserve-restart` |

A ultima linha tem ordem obrigatoria e o motivo esta em "O login unico, e o preco dele", mais abaixo.

O `docker-compose.yaml` da raiz e o ponto de entrada: ele so faz `include` de um arquivo por dominio. Cada dominio mora
na propria pasta, com os arquivos que **so ele** usa -- config, Dockerfile, script. Assim `docker compose config` mostra
o lab inteiro, mas abrir `kafka/cluster-a.yaml` mostra um cluster, nao os 21 servicos intercalados.

```
infra/
  kafka/          cluster-a.yaml  cluster-b.yaml  topics.yaml
                  Dockerfile  with-jmx.sh  jmx/kafka.yml
                  cluster-a/common.env  cluster-b/common.env  roles/*.env
                  scripts/create-topics.sh
  mirrormaker/    compose.yaml  mm2.properties
  kafbat/         compose.yaml  config.yaml
  dex/            compose.yaml  config.yaml
  openobserve/    compose.yaml
  otelcol/        compose.yaml  config.yaml
  flink/          compose.yaml  Dockerfile  sql/orders.sql
```

Duas dependencias atravessam pasta, e as duas estao comentadas onde acontecem: o MirrorMaker usa a imagem do Kafka
(`build: context: ../kafka`, porque o `connect-mirror-maker.sh` vem nela) e o Dex roda no namespace de rede do
OpenObserve.

|             |                                                   |                                                                           |
|-------------|---------------------------------------------------|---------------------------------------------------------------------------|
| kafbat      | http://localhost:8080                             | login `admin@admin` / `admin` (Dex)                                       |
| OpenObserve | http://localhost:5080                             | mesmo login do Dex, botão SSO; root local `admin@admin.com` / `Admin@123` |
| Flink       | http://localhost:8081                             | jobs, checkpoints, backpressure                                           |
| Dex (IdP)   | http://localhost:5556/dex                         | login unico do kafbat e do OpenObserve                                    |
| cluster-a   | `localhost:19092,localhost:19093,localhost:19094` |                                                                           |
| cluster-b   | `localhost:29092,localhost:29093,localhost:29094` |                                                                           |

O kafbat exige login: `auth.type: OAUTH2` apontando para o Dex, que roda no proprio lab (nenhuma credencial externa,
nenhuma chamada para internet). A configuracao completa -- clusters, metricas via Prometheus/JMX, auditoria, MCP, e um
`read-only` comentado -- esta em
[`kafbat/config.yaml`](kafbat/config.yaml), montado como arquivo estatico. O que a UI grava
(wizard, uploads) vai para o volume
`kafbat-etc`, em `/etc/kafkaui/dynamic_config.yaml`.

O endpoint MCP fica em `http://localhost:8080/mcp/sse` e herda o OAuth2 -- cliente sem fluxo OAuth leva 401. Para
conectar via proxy:

```bash
npx mcp-remote http://localhost:8080/mcp/sse --transport sse-only --allow-http
```

O historico temporal e o **OpenObserve** (logs, metricas e traces num binario so, com PromQL e SQL), alimentado pelo
**otelcol**, que traz tres coisas:

| sinal | de onde vem | onde aparece |
|---|---|---|
| metricas | scrape dos 14 alvos JMX na 7071 | Metrics -- ~550 streams `kafka_*`, `jvm_*` |
| logs | stdout dos 14 containers do Kafka | Logs -- stream `kafka_lab`, campo `service_name` |
| traces | OTLP das apps do host em `4317`/`4318` | Traces -- vazio ate uma app instrumentada rodar |

O Kafka nao fala OTLP e nao expoe log por rede: o que existe e o stdout que o docker grava em
`/var/lib/docker/containers/<id>/<id>-json.log`. Como o caminho e por id, nao da para filtrar no glob quais containers
ler -- entao os servicos do Kafka pedem ao driver `json-file` para gravar o label `com.docker.compose.service` em cada
linha, e o otelcol descarta no receiver tudo que vier sem esse label. E por isso que o collector roda como `root` com
`/var/lib/docker/containers` montado read-only. A alternativa seria montar o `docker.sock` para descobrir nomes, o que
daria controle total do host ao collector.

Para incluir outro servico nos logs, basta copiar o bloco `logging:` de `kafka/cluster-a.yaml` para ele. Os proprios
`otelcol` e `openobserve` ficam de fora de proposito: coletar o log de quem coleta cria realimentacao.

### O login unico, e o preco dele

O mesmo usuario do Dex entra no kafbat e no OpenObserve. Para isso o OpenObserve roda a build **enterprise**
(`public.ecr.aws/zinclabs/openobserve-enterprise`):
SSO nao existe na open source. Licenca comercial source-available, gratuita ate 50 GB/dia -- o lab faz alguns MB. Para
voltar ao AGPL puro, troque a imagem por
`openobserve/openobserve` e apague os `O2_DEX_*`; o login volta a ser so o root local.

O detalhe que obriga a uma gambiarra visivel: o OpenObserve tem **uma** URL para o Dex (`O2_DEX_BASE_URL`) e a usa nas
duas pontas -- o redirect do browser e a troca de token/JWKS feita pelo processo. Nenhum nome serve aos dois:
`localhost:5556` so vale no host, `dex:5556` so vale entre containers. Por isso o container do Dex roda no **namespace
de rede do OpenObserve**
(`network_mode: service:openobserve`), onde `localhost:5556` passa a valer dos dois lados; um alias de rede mantem o
nome `dex` resolvivel para o kafbat, que fala com o IdP de fora e aceita endpoints separados.

Consequencia pratica: todo restart ou recriacao do OpenObserve refaz o sandbox de rede, o Dex fica escutando num
namespace orfao e a 5556 para de responder no host. Por isso existe `make openobserve-restart`, que reinicia
OpenObserve, Dex e kafbat na ordem. O storage do Dex e sqlite num volume (`dex-data`) pelo mesmo motivo: com
`storage: memory` cada restart gera chaves de assinatura novas e o OpenObserve, que cacheia o JWKS por 10 minutos,
responde 401 ate o cache expirar.

O kafbat entra na lista por um terceiro motivo: o `WebClient` guarda conexoes para o Dex num pool que nao expira por
padrao. Depois de um restart do Dex, a conexao guardada esta morta e o POST de troca de token nao pode ser repetido --
o login falha com `Connection reset by peer` e 500 em `/login/oauth2/code/dex`. O `JAVA_OPTS` do servico expira conexoes
ociosas em 5s (`-Dreactor.netty.pool.maxIdleTime=5000`), entao o kafbat volta sozinho; a janela restante e so a de um
login disparado nos primeiros segundos apos o restart.

A ingestao do otelcol nao passa pelo SSO -- usa o usuario root em `Basic` auth. Token de browser nao serve para
processo.

## SQL sobre os topicos

```bash
make flink-example   # roda flink/sql/orders.sql: agrega orders.created por cliente
make flink-sql       # SQL client interativo
```

**Flink**, nao ksqlDB: ksqlDB so existe em imagem `confluentinc`, sob Confluent Community License, enquanto o Flink e
Apache-2.0. O preco da troca e a aba
"KSQL DB" do kafbat, que fala a REST API do ksqlDB e so ela -- por isso o
`ksqldb-server` esta comentado no config do kafbat. A interface do SQL passa a ser o client + a UI em `:8081`.

A imagem oficial do Flink nao traz conector nenhum; `flink/Dockerfile` adiciona o `flink-sql-connector-kafka`. As
versoes andam coladas: o sufixo do conector (`5.0.0-2.2`) **e** a minor do Flink, entao subir a imagem sem subir o
conector quebra o `CREATE TABLE`.

O exemplo em [`flink/sql/orders.sql`](flink/sql/orders.sql) usa
`'scan.bounded.mode' = 'latest-offset'` so para terminar sozinho. O caso normal do Flink e a query ficar de pe para
sempre: omita essa linha no client interativo e o resultado atualiza a cada evento novo.

O Flink entra nas duas redes, diferente do MM2 -- ele nao replica nada, le e calcula. Ler `orders.created` do cluster A
e `antifraud.analysis-completed` do cluster B no mesmo job e um join que **nao** passa pela fronteira do MM2, e vale
comparar com o caminho replicado.

## Portas

Dentro das redes docker todos os brokers usam `9092` (`kafka-broker-a-1:9092`) -- e o listener `INTERNAL`, usado pelo
MM2 e pelo kafbat. As portas `x909x` acima sao o listener `EXTERNAL`, publicado no host porque seis processos nao cabem
na mesma porta:

```
19092  broker-a-1        29092  broker-b-1
19093  broker-a-2        29093  broker-b-2
19094  broker-a-3        29094  broker-b-3
```

Primeiro digito = cluster, ultimo = numero do broker. Nao existe `19091`.

**Configure os tres endereços, nao so um.** `bootstrap.servers` e a lista de contato inicial: com so `localhost:19092`,
parar o `broker-a-1` (exercicio 1)
impede o cliente ate de descobrir o cluster, mesmo com os outros dois no ar.

## Topicos

| Cluster | Topico                                                                             | Quem cria                  |
|---------|------------------------------------------------------------------------------------|----------------------------|
| A       | `orders.created`, `orders.validated`, `orders.fraud-requested`, `orders.processed` | `scripts/create-topics.sh` |
| A       | `cluster-b.antifraud.analysis-completed`                                           | MirrorMaker 2              |
| B       | `antifraud.analysis-completed`                                                     | `scripts/create-topics.sh` |
| B       | `cluster-a.orders.fraud-requested`                                                 | MirrorMaker 2              |

O prefixo `cluster-a.` **e o nome real do topico dentro do cluster B** -- nao e endereco nem namespace de rede. Quem
consome do outro lado pede o nome prefixado; `orders.fraud-requested` simplesmente nao existe no cluster B.

## Exercicios de falha

```bash
make kill-broker CLUSTER=a N=2      # 1 broker fora: ISR cai para 2, producao segue
make kill-broker CLUSTER=a N=3      # 2 brokers fora: acks=all falha, NOT_ENOUGH_REPLICAS
make start-broker CLUSTER=a N=2

make kill-controller CLUSTER=a N=1  # plano de controle tolera perder 1 de 3
make start-controller CLUSTER=a N=1

make kill-cluster CLUSTER=b         # cluster inteiro fora: lag do MM2 = o RPO
make start-cluster CLUSTER=b

make kill-mirror N=1                # 1 instancia do MM2 fora: a outra assume as tasks
make start-mirror N=1

make observability-down             # libera RAM parando kafbat, dex,
                                    # openobserve e otelcol
make observability-up
make flink-down                     # libera ~1Gi parando o Flink
make flink-up

make quorum CLUSTER=a               # estado do quorum KRaft
make describe CLUSTER=a             # ISR de todos os topicos
make ps
```

`kill-*` usa `docker compose stop`, nao `down`: os volumes ficam, entao da para observar o ISR sendo reconstruido e o
MM2 drenando o lag acumulado ao restaurar.

## Detalhes que costumam surpreender

**Ferramentas CLI e o javaagent.** O `-javaagent` do Prometheus e injetado por
`with-jmx.sh`, nao pelo environment do container. Environment e herdado por
`docker exec`, e toda ferramenta CLI morreria com `BindException: Address in use`
tentando bindar a 7071 que o servidor ja ocupa.

**Isolamento vale entre containers, nao a partir do host.** As apps rodam no host e alcancam `localhost:19092` e
`localhost:29092`. Se uma app apontar para o cluster errado, ela conecta e funciona quase -- a rede nao avisa. O
isolamento garante que o MM2 seja a unica ponte de dados, nao que voce nao erre a config.

**EOS nao atravessa cluster.** O MM2 e um consumer comum lendo de um cluster e um producer comum escrevendo no outro:
replicacao at-least-once, duplicatas cruzam a fronteira. E por isso que a regra de idempotencia por `orderId` e
obrigatoria.

## Limpar

```bash
make down     # para tudo, preserva volumes
make clean    # apaga volumes e redes tambem
```
