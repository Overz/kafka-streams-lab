## 1. Contratos e infraestrutura do change

- [x] 1.1 Acrescentar o record `OrderDecided` em `shared/event`
- [x] 1.2 Acrescentar a fabrica `HeaderCleanup` em `shared/streams`, com a implementacao padrao apoiada no `HeaderCleanupProcessorSupplier`
- [x] 1.3 Acrescentar `orders.approved`, `orders.rejected` e `orders.dlq` ao array `TOPICS_A` de `infra/kafka/scripts/create-topics.sh`
- [x] 1.4 Completar `infra/config-server/repository/orders.yaml` com o que o pipeline le
- [x] 1.5 Acrescentar MapStruct e as dependencias de teste a `orders/pom.xml`
- [x] 1.6 Remover `orders/.../Main.java` (template) e `orders/.../processors/ErrorHandler.java`
- [x] 1.7 Verificar: `mvn -pl orders -am compile` passa

## 2. Regras de validacao

- [x] 2.1 Teste: cada regra isolada, sem Kafka -- obrigatorios, itens, quantidade, preco, total positivo, coerencia do total, escala diferente
- [x] 2.2 Teste: as regras sao todas aplicadas e os motivos acumulam
- [x] 2.3 Implementar a interface `OrderValidationRule` e as regras
- [x] 2.4 Verificar: `mvn -pl orders test` verde

## 3. Etapas do pipeline

- [x] 3.1 Teste: `ValidationProcessor` com `TopologyTestDriver` -- aceita valido, rejeita com motivo acumulado, serializa o payload original na falha
- [x] 3.2 Teste: `DeadLetterMapper` monta o `DeadLetter` e promove os auxiliares para `x-error-*`
- [x] 3.3 Teste: `DecisionJoiner` combina decisao e estado, tirando valor e moeda do estado
- [x] 3.4 Teste: `CompletionMapper` converte `OrderDecided` em `OrderProcessed`
- [x] 3.5 Implementar o mapper MapStruct `OrdersMapper`
- [x] 3.6 Implementar `ValidationProcessorSupplier`, `FraudRequestSupplier`, `DecisionJoiner`, `CompletionMapper`, `DeadLetterMapper` e `ApprovedPredicate`
- [x] 3.7 Verificar: `mvn -pl orders test` verde

## 4. Topologia

- [x] 4.1 Teste: a topologia inteira com `TopologyTestDriver` -- caminho feliz do `orders.created` ao `orders.processed`
- [x] 4.2 Teste: pedido invalido termina em `orders.dlq` e em nenhum outro topico
- [x] 4.3 Teste: decisao sem pedido correspondente nao produz nada
- [x] 4.4 Teste: aprovado vai so para `orders.approved`; rejeitado so para `orders.rejected`; ambos chegam a `orders.processed`
- [x] 4.5 Teste: nenhum dos seis topicos de saida carrega header `x-internal-`, e `x-request-id` e `traceparent` sobrevivem
- [x] 4.6 Teste: substituir a etapa de validacao por uma que rejeita tudo nao exige alterar a topologia
- [x] 4.7 Implementar os records `OrdersTopics`, `OrdersSerdes` e `OrdersStages`
- [x] 4.8 Implementar `OrdersTopology`
- [x] 4.9 Verificar: `mvn -pl orders test` verde

## 5. Aplicacao

- [x] 5.1 Implementar `OrdersConfig` estendendo `AppConfig`
- [x] 5.2 Implementar o `Main`: le a config, inicializa a telemetria, monta as etapas, entrega ao `StreamsRunner`
- [x] 5.3 Acrescentar os alvos `orders` e `apps-up` ao `Makefile`
- [x] 5.4 Verificar: `make orders` sobe a aplicacao contra o lab e ela chega a `RUNNING`

## 6. Verificacao do change

- [x] 6.1 `mvn clean verify -DskipITs` verde no projeto inteiro
- [x] 6.2 Publicar um pedido valido pela API Go e ver `orders.validated` e `orders.fraud-requested` receberem
- [x] 6.3 Publicar um pedido com `totalAmount` divergente e ver `orders.dlq` receber com os headers de erro
- [x] 6.4 `openspec validate pipeline-orders --strict` sem erro
