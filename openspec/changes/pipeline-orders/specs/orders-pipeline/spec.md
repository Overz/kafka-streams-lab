## ADDED Requirements

### Requirement: Pedido de analise antifraude
Todo pedido validado SHALL originar um `OrderForFraudAnalysis` publicado em
`orders.fraud-requested`, com a mesma chave do pedido. O `orders` SHALL NOT conhecer como a
analise e feita.

#### Scenario: Validado gera pedido de analise
- **WHEN** um `OrderValidated` e publicado em `orders.validated`
- **THEN** um `OrderForFraudAnalysis` e publicado em `orders.fraud-requested` com a mesma chave
- **AND** ele carrega `orderId`, `customerId`, `totalAmount`, `currency` e `createdAt`

#### Scenario: Chave particiona por pedido
- **WHEN** um `OrderForFraudAnalysis` e publicado
- **THEN** a chave do registro e o `orderId`, para que todos os eventos de um pedido mantenham a ordem

### Requirement: Estado do pedido disponivel para a decisao
O topico `orders.validated` SHALL ser lido como `KTable` materializada, de modo que o estado
de cada pedido esteja disponivel quando a decisao do antifraude chegar.

#### Scenario: Estado alimenta o evento final
- **WHEN** a decisao do antifraude chega para um pedido ja validado
- **THEN** o `totalAmount` e a `currency` do evento final vem do estado guardado, e nao da decisao

#### Scenario: Ultima versao do pedido prevalece
- **WHEN** dois `OrderValidated` com a mesma chave sao publicados em sequencia
- **THEN** o estado consultado no join e o do segundo

### Requirement: Cruzamento entre decisao e pedido
A `FraudAnalysisCompleted` lida do topico de decisao SHALL ser cruzada com o estado do pedido
por `orderId`, produzindo um `OrderDecided`. O nome do topico de decisao SHALL vir da
configuracao, porque o MirrorMaker 2 o entrega prefixado com o alias do cluster de origem.

#### Scenario: Decisao encontra o pedido
- **WHEN** uma `FraudAnalysisCompleted` chega para um pedido presente no estado
- **THEN** um `OrderDecided` e produzido com a decisao, o `riskScore` e as razoes da analise
- **AND** com o `totalAmount` e a `currency` vindos do estado do pedido

#### Scenario: Decisao sem pedido correspondente
- **WHEN** uma `FraudAnalysisCompleted` chega para um `orderId` ausente do estado
- **THEN** nada e publicado em `orders.approved`, `orders.rejected` ou `orders.processed`

#### Scenario: Topico de decisao prefixado pelo MirrorMaker 2
- **WHEN** a configuracao aponta a decisao para `cluster-b.antifraud.analysis-completed`
- **THEN** a topologia consome desse topico
- **AND** a mesma topologia consome de `antifraud.analysis-completed` quando a configuracao assim indicar

### Requirement: Separacao entre aprovado e rejeitado
Um `OrderDecided` SHALL ser encaminhado a `orders.approved` quando a decisao for `APPROVED` e
a `orders.rejected` quando for `REJECTED`. Um mesmo pedido SHALL aparecer em apenas um dos dois.

#### Scenario: Pedido aprovado
- **WHEN** a decisao do antifraude e `APPROVED`
- **THEN** um `OrderDecided` e publicado em `orders.approved`
- **AND** nada e publicado em `orders.rejected`

#### Scenario: Pedido rejeitado
- **WHEN** a decisao do antifraude e `REJECTED`
- **THEN** um `OrderDecided` e publicado em `orders.rejected` carregando as razoes da analise
- **AND** nada e publicado em `orders.approved`

### Requirement: Evento final do ciclo
Os dois ramos da decisao SHALL convergir num unico `OrderProcessed` publicado em
`orders.processed`, que marca o fim do ciclo de vida do pedido independentemente do desfecho.

#### Scenario: Aprovado chega ao evento final
- **WHEN** um pedido e aprovado
- **THEN** um `OrderProcessed` com decisao `APPROVED` e publicado em `orders.processed`

#### Scenario: Rejeitado chega ao evento final
- **WHEN** um pedido e rejeitado
- **THEN** um `OrderProcessed` com decisao `REJECTED` e publicado em `orders.processed`
- **AND** ele preserva o `riskScore` e as razoes da analise

#### Scenario: Um evento final por pedido
- **WHEN** um pedido percorre o pipeline inteiro
- **THEN** exatamente um `OrderProcessed` e publicado para aquele `orderId`

### Requirement: Topologia composta de pipelines independentes
A topologia do dominio SHALL ser uma lista de pipelines, cada uma lendo de um ou mais
topicos e escrevendo em um ou mais topicos. Uma pipeline SHALL NOT referenciar outra:
o encadeamento entre elas acontece pelo topico, nunca por chamada de codigo.

#### Scenario: Encadeamento pelo topico
- **WHEN** a pipeline de validacao publica em `orders.validated`
- **THEN** a pipeline de pedido de analise consome desse topico sem que nenhuma das duas conheca a outra

#### Scenario: Acrescentar uma etapa
- **WHEN** uma etapa nova precisa entrar no fluxo
- **THEN** basta acrescentar uma pipeline a lista, sem alterar as existentes

#### Scenario: Topico lido por duas pipelines
- **WHEN** duas pipelines declaram o mesmo topico de entrada
- **THEN** a topologia e montada com um unico source, compartilhado entre elas
- **AND** os headers do registro original chegam as duas

### Requirement: Montagem independente de implementacao
O codigo que monta as pipelines SHALL receber apenas tipos declarados pelo Kafka Streams e
nomes vindos da configuracao. Ele SHALL NOT referenciar nenhuma classe concreta de regra de
negocio, validacao ou conversao.

#### Scenario: Nenhum tipo de dominio na montagem
- **WHEN** o arquivo que monta as pipelines e inspecionado
- **THEN** ele nao importa nenhuma classe de regra, mapper ou etapa concreta
- **AND** cada etapa recebida e um `ProcessorSupplier`, `FixedKeyProcessorSupplier` ou `ValueJoiner`

#### Scenario: Substituir uma etapa nao altera as pipelines
- **WHEN** um teste monta as pipelines com uma etapa de validacao que rejeita tudo
- **THEN** as pipelines sao construidas sem nenhuma alteracao no seu codigo
- **AND** todo pedido termina em `orders.dlq`

### Requirement: Nenhum header auxiliar nos topicos de saida
Todo registro publicado pelo pipeline SHALL passar por uma etapa de limpeza imediatamente
antes do sink. Nenhum dos seis topicos de saida SHALL receber header com prefixo `x-internal-`.

#### Scenario: Saidas limpas
- **WHEN** um pedido percorre o pipeline inteiro
- **THEN** nenhum registro em `orders.validated`, `orders.fraud-requested`, `orders.approved`, `orders.rejected`, `orders.processed` ou `orders.dlq` carrega header iniciado por `x-internal-`

#### Scenario: Rastreio de entrada sobrevive
- **WHEN** o `OrderCreated` chega com `x-request-id` e `traceparent`
- **THEN** o `OrderProcessed` correspondente ainda carrega os dois headers
