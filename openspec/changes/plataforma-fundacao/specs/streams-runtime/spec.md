## ADDED Requirements

### Requirement: Ciclo de vida da aplicacao Streams
O `shared` SHALL expor um `StreamsRunner` que recebe uma `Topology` e as `Properties` do Kafka
Streams e cuida de start, parada limpa e tratamento de excecao nao capturada. O runner SHALL
registrar um shutdown hook e SHALL fechar a instancia com timeout explicito.

#### Scenario: Parada por sinal do sistema
- **WHEN** o processo recebe SIGTERM
- **THEN** o runner fecha a instancia de Streams aguardando ate o timeout configurado
- **AND** registra em log o estado final da aplicacao

#### Scenario: Transicao de estado
- **WHEN** a aplicacao passa de `REBALANCING` para `RUNNING`
- **THEN** a transicao e registrada em log com o estado anterior e o novo

#### Scenario: Excecao nao capturada numa task
- **WHEN** uma excecao escapa do processamento de um registro
- **THEN** o handler configurado decide entre substituir a thread e encerrar a aplicacao
- **AND** a decisao e registrada em log com a causa

### Requirement: Pipeline como unidade de composicao
O `shared` SHALL expor uma abstracao de pipeline que qualquer dominio compoe em lista para
formar sua topologia, e as formas reutilizaveis dessa abstracao: transformacao simples,
divisao em varias saidas e cruzamento com estado.

#### Scenario: Topologia a partir de uma lista
- **WHEN** um dominio entrega uma lista de pipelines
- **THEN** a topologia e construida chamando cada uma em ordem
- **AND** nenhum dominio precisa de uma classe propria de topologia

#### Scenario: Source unico por topico
- **WHEN** duas pipelines da mesma topologia declaram o mesmo topico de entrada
- **THEN** um unico source e criado e compartilhado
- **AND** a construcao nao falha com "has already been registered by another source"

#### Scenario: Tabela e fluxo do mesmo topico
- **WHEN** uma pipeline precisa do topico como tabela e outra como fluxo
- **THEN** ambas sao atendidas a partir do mesmo source
- **AND** o fluxo preserva os headers do registro original

### Requirement: Higiene dos headers de saida
Headers auxiliares trocados entre etapas SHALL usar o prefixo `x-internal-`. Nenhum registro
escrito em um topico de saida SHALL conter header com esse prefixo. A remocao SHALL acontecer
numa etapa dedicada imediatamente antes de cada sink, aplicada a todos os ramos.

#### Scenario: Registro de sucesso chega limpo ao topico
- **WHEN** um evento atravessa etapas que adicionam `x-internal-stage` e `x-internal-received-at`
- **THEN** o registro escrito no topico de saida nao contem nenhum header iniciado por `x-internal-`

#### Scenario: Headers de entrada sao preservados
- **WHEN** o registro de entrada carrega `x-request-id` e `traceparent`
- **THEN** o registro de saida ainda carrega esses dois headers com os mesmos valores

#### Scenario: Registro de erro preserva o diagnostico
- **WHEN** um evento e roteado para a DLQ
- **THEN** o registro na DLQ carrega `x-error-reason`, `x-error-stage` e `x-original-topic`
- **AND** nao carrega nenhum header iniciado por `x-internal-`

#### Scenario: Chave preservada na limpeza
- **WHEN** a etapa de limpeza de headers processa um registro
- **THEN** a chave do registro permanece inalterada

#### Scenario: Limpeza nao depende de quem monta
- **WHEN** uma saida nova e acrescentada a uma pipeline
- **THEN** a limpeza e aplicada sem que a montagem precise pedir por ela
