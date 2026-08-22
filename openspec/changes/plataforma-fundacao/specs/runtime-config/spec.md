## ADDED Requirements

### Requirement: Configuracao servida pelo config-server
O lab SHALL prover um Spring Cloud Config Server com backend `native`, servindo arquivos
`.yaml` versionados. O servidor SHALL responder na porta 8888 e SHALL compor a resposta a
partir de `application.yaml` (comum) e `<app>.yaml` (por aplicacao).

#### Scenario: Configuracao de uma aplicacao conhecida
- **WHEN** um cliente faz `GET /orders/local`
- **THEN** a resposta HTTP e 200
- **AND** o corpo contem um `propertySources` com as entradas de `orders.yaml` antes das de `application.yaml`

#### Scenario: Aplicacao desconhecida
- **WHEN** um cliente faz `GET /inexistente/local`
- **THEN** a resposta HTTP e 200 com apenas as propriedades de `application.yaml`

### Requirement: Carga da configuracao no boot
Cada aplicacao Streams SHALL buscar sua configuracao do config-server durante a inicializacao,
antes de construir a topologia, e SHALL desserializa-la em records tipados acessiveis por uma
interface. Nenhuma leitura de configuracao SHALL ocorrer depois do start da topologia.

#### Scenario: Precedencia entre fontes
- **WHEN** a mesma chave existe em `application.yaml` e em `orders.yaml`
- **THEN** o valor efetivo e o de `orders.yaml`

#### Scenario: Chave aninhada
- **WHEN** o config-server devolve a propriedade achatada `kafka.bootstrap-servers`
- **THEN** ela e acessivel como `config.kafka().bootstrapServers()`

#### Scenario: Lista com um unico elemento
- **WHEN** uma propriedade de lista contem um unico endereco de broker
- **THEN** o record recebe uma lista de um elemento, nao uma string com virgulas

### Requirement: Falha rapida em configuracao invalida
Configuracao obrigatoria ausente ou mal formada SHALL derrubar o processo durante o boot,
com uma mensagem que nomeia a chave e a aplicacao. A aplicacao SHALL NOT iniciar a topologia
com configuracao parcial.

#### Scenario: Chave obrigatoria ausente
- **WHEN** `kafka.bootstrap-servers` nao esta em nenhuma fonte
- **THEN** o processo termina com codigo diferente de zero
- **AND** a mensagem de erro contem o nome da chave ausente

#### Scenario: Tipo incompativel
- **WHEN** uma propriedade que deveria ser duracao contem texto que nao e duracao
- **THEN** o processo termina no boot
- **AND** a mensagem nomeia a chave e o valor recebido

#### Scenario: Config-server indisponivel
- **WHEN** o config-server nao responde
- **THEN** o cliente tenta novamente com backoff ate um limite configurado
- **AND** ao esgotar as tentativas o processo termina com uma mensagem contendo a URL tentada

### Requirement: Nomes de topico e de state store vem da configuracao
Nenhum nome de topico Kafka ou de state store SHALL ser constante no codigo Java. Todos SHALL
vir da configuracao, porque o nome do topico de entrada cross-dominio depende do prefixo que o
MirrorMaker 2 aplica.

#### Scenario: Antifraud no lab com dois clusters
- **WHEN** a configuracao aponta o topico de entrada do antifraud para `cluster-a.orders.fraud-requested`
- **THEN** a topologia le desse topico sem qualquer alteracao de codigo

#### Scenario: Antifraud no teste e2e com um broker
- **WHEN** a configuracao aponta o mesmo topico para `orders.fraud-requested`
- **THEN** a mesma topologia le do topico sem prefixo
