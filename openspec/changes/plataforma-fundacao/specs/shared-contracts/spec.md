## ADDED Requirements

### Requirement: Eventos do dominio como records
O modulo `shared` SHALL expor cada evento do documento de dominio como um Java record anotado
com `@Jacksonized`, `@Builder` e `@With`. Os nomes dos campos SHALL corresponder exatamente ao
JSON descrito em `Kafka Streams Lab.md`, sem campo extra e sem campo omitido.

#### Scenario: OrderCreated reflete o contrato do documento
- **WHEN** o record `OrderCreated` e inspecionado
- **THEN** ele tem exatamente os componentes `orderId`, `customerId`, `items`, `totalAmount`, `currency` e `createdAt`
- **AND** `items` e uma `List<OrderItem>` com componentes `productId`, `quantity` e `unitPrice`

#### Scenario: FraudAnalysisCompleted reflete o contrato do documento
- **WHEN** o record `FraudAnalysisCompleted` e inspecionado
- **THEN** ele tem exatamente os componentes `orderId`, `customerId`, `decision`, `riskScore`, `reasons` e `analyzedAt`
- **AND** `decision` e o enum `Decision` com os valores `APPROVED` e `REJECTED`
- **AND** `reasons` e uma `List<FraudReason>` cujos valores incluem `HIGH_ORDER_AMOUNT`, `TOO_MANY_ORDERS_IN_SHORT_PERIOD` e `AMOUNT_ABOVE_CUSTOMER_PROFILE`

#### Scenario: OrderValidated preserva a data de criacao
- **WHEN** o record `OrderValidated` e inspecionado
- **THEN** ele tem `createdAt` alem de `validatedAt`
- **AND** o `OrderForFraudAnalysis` derivado dele carrega a data de criacao do pedido, nao a da validacao

#### Scenario: Construcao incremental com @With
- **WHEN** um `OrderCreated` existente recebe `withTotalAmount(novoValor)`
- **THEN** um novo record e devolvido com o valor trocado
- **AND** a instancia original permanece inalterada

### Requirement: Valores monetarios em BigDecimal
Todo campo que representa dinheiro SHALL ser `BigDecimal`. Comparacoes entre valores monetarios
SHALL usar `compareTo`, nunca `equals`, porque escalas diferentes representam o mesmo valor.

#### Scenario: Soma de itens sem erro de ponto flutuante
- **WHEN** um pedido tem itens de `0.10` e `0.20`
- **THEN** a soma comparada com `0.30` por `compareTo` devolve zero

#### Scenario: Escalas diferentes representam o mesmo valor
- **WHEN** `300.00` e comparado com `300.0` por `compareTo`
- **THEN** o resultado e zero

### Requirement: Serializacao JSON para o Kafka
O `shared` SHALL expor `JsonSerde<T>` implementando `org.apache.kafka.common.serialization.Serde`.
Instantes SHALL ser serializados em ISO-8601 UTC. Campos desconhecidos na leitura SHALL ser
ignorados, e nao causar falha.

#### Scenario: Round-trip preserva o evento
- **WHEN** um `OrderCreated` e serializado e desserializado pelo `JsonSerde`
- **THEN** o record resultante e igual ao original, incluindo a escala dos `BigDecimal`

#### Scenario: Instante em ISO-8601
- **WHEN** um evento com `createdAt` igual a `2026-08-07T18:30:00Z` e serializado
- **THEN** o JSON contem a string `"2026-08-07T18:30:00Z"`
- **AND** nao contem um numero de epoch

#### Scenario: Produtor adiciona um campo novo
- **WHEN** o JSON de entrada tem um campo que o record nao declara
- **THEN** a desserializacao ignora o campo e devolve o record preenchido
- **AND** nao lanca excecao

#### Scenario: Payload corrompido
- **WHEN** os bytes de entrada nao sao JSON valido
- **THEN** o `JsonSerde` lanca `SerializationException` com o nome do topico na mensagem

#### Scenario: Tombstone
- **WHEN** o valor recebido e `null`
- **THEN** a desserializacao devolve `null` sem lancar excecao

### Requirement: Resultado de etapa como valor
O `shared` SHALL expor `StageOutcome<T>` como `sealed interface` com as implementacoes
`Success<T>` e `Failure`. Uma etapa que rejeita um evento SHALL devolver `Failure`, nunca
lancar excecao, para que a topologia possa rotear o evento sem derrubar a task.

#### Scenario: Etapa rejeita evento invalido
- **WHEN** uma etapa recebe um evento que viola uma regra de negocio
- **THEN** ela devolve um `Failure` contendo o motivo, o nome da etapa e o payload original

#### Scenario: Falha carrega o dado para reprocessamento
- **WHEN** uma etapa produz um `Failure`
- **THEN** o payload original serializado acompanha a falha
- **AND** um registro na DLQ montado a partir dele contem o evento inteiro, nao apenas o motivo

#### Scenario: Pattern matching exaustivo
- **WHEN** um `switch` sobre `StageOutcome` cobre `Success` e `Failure`
- **THEN** a compilacao aceita sem ramo `default`
