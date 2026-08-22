## Why

O modulo `orders` compila mas nao faz nada: tem um `Main.java` de template do IntelliJ e um
`ErrorHandler.java` vazio. A fundacao construida no change `plataforma-fundacao` -- eventos,
serdes, configuracao, runtime de Streams, telemetria -- existe justamente para que este
pipeline possa ser escrito, e ate agora nenhum evento e processado.

O `orders` e o dono do ciclo de vida do pedido. Ele nao decide se um pedido e fraudulento;
ele valida, pede a analise, recebe a decisao e produz o resultado final. Sem ele, o
`antifraud` nao tem o que analisar.

## What Changes

- **Pipeline completo do dominio orders**, em etapas independentes:
  1. valida o `OrderCreated` que chega em `orders.created`;
  2. publica `OrderValidated` em `orders.validated`;
  3. projeta e publica `OrderForFraudAnalysis` em `orders.fraud-requested`;
  4. le a decisao do antifraude em `cluster-b.antifraud.analysis-completed`;
  5. cruza a decisao com o estado do pedido e produz `OrderDecided`;
  6. separa em `orders.approved` e `orders.rejected`;
  7. reune os dois ramos e publica `OrderProcessed` em `orders.processed`.
- **Dead letter queue** em `orders.dlq` para todo evento que nao passa na validacao,
  com os headers `x-error-reason`, `x-error-stage`, `x-error-detail` e `x-original-topic`.
- **Novo evento `OrderDecided`** no `shared`, publicado em `orders.approved` e `orders.rejected`.
- **`OrdersTopology`** recebe exclusivamente contratos do Kafka Streams. As implementacoes de
  validacao, projecao, decisao e finalizacao sao montadas no `Main` e entregam-se a ela como
  `ProcessorSupplier`, `FixedKeyProcessorSupplier`, `ValueJoiner`, `Predicate` e `ValueMapper`.
- **Conversoes entre eventos por MapStruct**, nao por construcao manual campo a campo.
- **Tres topicos novos** em `create-topics.sh`: `orders.approved`, `orders.rejected`, `orders.dlq`.
- **Remocao** do `Main.java` de template e do `ErrorHandler.java` vazio.

## Capabilities

### New Capabilities

- `order-validation`: quais pedidos sao aceitos, quais sao rejeitados, e o que acontece
  com os rejeitados.
- `orders-pipeline`: a forma do pipeline do dominio orders -- as etapas, o que cada topico
  recebe, e como a decisao do antifraude se junta ao estado do pedido.

### Modified Capabilities

Nenhuma. As capabilities do `plataforma-fundacao` sao usadas, nao alteradas.

## Impact

**Codigo novo**: `orders/.../OrdersTopology`, `OrdersStages`, `OrdersSerdes`, `OrdersTopics`,
`OrdersConfig`, o pacote `stage/`, o pacote `rule/` e o mapper MapStruct.
`OrderDecided` em `shared/event`. `HeaderCleanup` em `shared/streams`.

**Codigo alterado**: `orders/pom.xml`; `infra/kafka/scripts/create-topics.sh`;
`infra/config-server/repository/orders.yaml`.

**Codigo removido**: `orders/.../Main.java` (template) e `orders/.../processors/ErrorHandler.java`.

**Topicos novos** no cluster A: `orders.approved`, `orders.rejected`, `orders.dlq`.

**Dependencia operacional**: o `orders` so produz `OrderProcessed` quando o `antifraud`
estiver publicando em `antifraud.analysis-completed` e o MirrorMaker 2 replicando para o
cluster A. Ate o change `pipeline-antifraud`, o pipeline para no passo 3.
