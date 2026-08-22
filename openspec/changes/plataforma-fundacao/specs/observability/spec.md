## ADDED Requirements

### Requirement: Exportacao OTLP
Cada aplicacao Streams SHALL inicializar o SDK do OpenTelemetry no boot e exportar trace,
metrica e log por OTLP para o collector do lab. O endpoint SHALL vir da configuracao.

#### Scenario: Recurso identifica a aplicacao
- **WHEN** a aplicacao `orders` exporta qualquer sinal
- **THEN** o recurso associado tem `service.name` igual a `orders`

#### Scenario: Collector indisponivel
- **WHEN** o collector nao responde na porta configurada
- **THEN** a aplicacao continua processando eventos normalmente
- **AND** a falha de exportacao e registrada em log sem derrubar a topologia

### Requirement: Logs correlacionados e ricos em contexto
Todo log SHALL ser emitido por SLF4J e SHALL ser tambem exportado por OTLP. Mensagens de log de
etapa SHALL nomear os identificadores relevantes do evento como parametros.

#### Scenario: Log dentro de um span
- **WHEN** uma etapa registra log durante o processamento de um evento
- **THEN** o registro exportado carrega o `trace_id` e o `span_id` do span ativo

#### Scenario: Contexto do evento na mensagem
- **WHEN** uma etapa registra o inicio do processamento
- **THEN** a mensagem nomeia ao menos `orderId` e a etapa, como parametros e nao concatenados

### Requirement: Metricas nativas do Kafka Streams
As metricas expostas por `KafkaStreams.metrics()` SHALL ser publicadas como instrumentos
OpenTelemetry, para que apareçam no mesmo backend das metricas JMX dos brokers.

#### Scenario: Metrica numerica do Streams vira gauge
- **WHEN** a ponte de metricas coleta uma metrica de valor numerico do Streams
- **THEN** um gauge OTel e observado com o mesmo valor
- **AND** as tags da metrica viram atributos do gauge

#### Scenario: Metrica nao numerica
- **WHEN** uma metrica do Streams tem valor nao numerico, como o estado da thread
- **THEN** a ponte a ignora sem lancar excecao

### Requirement: Span por etapa do pipeline
Cada etapa do pipeline SHALL abrir um span nomeado, com atributos que identifiquem o evento
processado, e SHALL encerra-lo mesmo em caso de falha.

#### Scenario: Etapa bem sucedida
- **WHEN** uma etapa processa um evento com sucesso
- **THEN** o span correspondente encerra com status de sucesso
- **AND** carrega o atributo com o identificador do pedido

#### Scenario: Etapa que rejeita o evento
- **WHEN** uma etapa devolve falha para um evento invalido
- **THEN** o span encerra com status de erro
- **AND** carrega o motivo da rejeicao como atributo
