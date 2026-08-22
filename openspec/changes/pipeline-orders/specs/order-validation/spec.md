## ADDED Requirements

### Requirement: Campos obrigatorios do pedido
Um `OrderCreated` recebido em `orders.created` SHALL ser rejeitado se qualquer campo
obrigatorio estiver ausente ou vazio: `orderId`, `customerId`, `currency`, `createdAt`, e ao
menos um item. Cada item SHALL ter `productId` preenchido, `quantity` maior que zero e
`unitPrice` maior que zero.

#### Scenario: Pedido completo e aceito
- **WHEN** um `OrderCreated` com todos os campos preenchidos chega em `orders.created`
- **THEN** um `OrderValidated` e publicado em `orders.validated` com a mesma chave
- **AND** nada e publicado em `orders.dlq`

#### Scenario: Pedido sem orderId
- **WHEN** um `OrderCreated` sem `orderId` chega em `orders.created`
- **THEN** nada e publicado em `orders.validated`
- **AND** um registro e publicado em `orders.dlq` com `x-error-reason` contendo `ORDER_ID_REQUIRED`

#### Scenario: Pedido sem itens
- **WHEN** um `OrderCreated` com lista de itens vazia chega em `orders.created`
- **THEN** um registro e publicado em `orders.dlq` com `x-error-reason` contendo `ITEMS_REQUIRED`

#### Scenario: Item com quantidade zero
- **WHEN** um item do pedido tem `quantity` igual a zero
- **THEN** um registro e publicado em `orders.dlq` com `x-error-reason` contendo `ITEM_QUANTITY_MUST_BE_POSITIVE`

#### Scenario: Item com preco zero
- **WHEN** um item do pedido tem `unitPrice` igual a zero
- **THEN** um registro e publicado em `orders.dlq` com `x-error-reason` contendo `ITEM_UNIT_PRICE_MUST_BE_POSITIVE`

#### Scenario: Varios campos invalidos ao mesmo tempo
- **WHEN** um `OrderCreated` chega sem `orderId` e sem `currency`
- **THEN** o registro na DLQ carrega os dois motivos, e nao apenas o primeiro encontrado

### Requirement: Coerencia do valor total
O `totalAmount` do pedido SHALL ser igual a soma de `quantity` x `unitPrice` de todos os
itens. A comparacao SHALL ser feita em `BigDecimal`, e nao em ponto flutuante.

#### Scenario: Total coerente com os itens
- **WHEN** um pedido tem dois itens de 2 x 150.00 e 1 x 100.00 e `totalAmount` igual a 400.00
- **THEN** o pedido e validado

#### Scenario: Total divergente
- **WHEN** um pedido tem um item de 2 x 150.00 e `totalAmount` igual a 999.00
- **THEN** um registro e publicado em `orders.dlq` com `x-error-reason` contendo `TOTAL_AMOUNT_MISMATCH`
- **AND** o header `x-error-detail` nomeia o valor esperado e o recebido

#### Scenario: Total com escala diferente
- **WHEN** um pedido tem itens somando 300.00 e `totalAmount` igual a 300.0
- **THEN** o pedido e validado, porque a escala nao muda o valor

#### Scenario: Total zerado
- **WHEN** um pedido tem `totalAmount` igual a zero
- **THEN** um registro e publicado em `orders.dlq` com `x-error-reason` contendo `TOTAL_AMOUNT_MUST_BE_POSITIVE`

### Requirement: Conteudo do registro na dead letter queue
Um registro publicado em `orders.dlq` SHALL preservar o evento original serializado e
SHALL carregar em headers o motivo, a etapa, o detalhe e o topico de origem.

#### Scenario: Registro na DLQ permite reprocessamento
- **WHEN** um pedido invalido e roteado para `orders.dlq`
- **THEN** o valor do registro contem o JSON do `OrderCreated` original
- **AND** os headers `x-error-reason`, `x-error-stage` e `x-original-topic` estao presentes
- **AND** `x-original-topic` vale `orders.created`

#### Scenario: DLQ nao vaza header auxiliar
- **WHEN** um pedido invalido e roteado para `orders.dlq`
- **THEN** nenhum header do registro comeca com `x-internal-`

### Requirement: Regras de validacao extensiveis
As regras de validacao SHALL ser unidades independentes, cada uma respondendo por um unico
motivo de rejeicao, aplicadas todas antes de decidir. Acrescentar uma regra SHALL NOT exigir
alteracao na etapa que as aplica.

#### Scenario: Regra isolada avalia sem Kafka
- **WHEN** uma regra de validacao e invocada diretamente com um pedido
- **THEN** ela devolve o motivo da rejeicao, ou vazio se o pedido a satisfaz

#### Scenario: Todas as regras sao aplicadas
- **WHEN** um pedido viola tres regras
- **THEN** o resultado da etapa acumula os tres motivos
