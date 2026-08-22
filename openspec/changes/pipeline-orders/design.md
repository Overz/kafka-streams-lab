## Context

A fundacao ja existe: eventos, serdes, config tipada, `StreamsRunner`, `HeaderCleanupProcessor`
e telemetria. Falta o pipeline.

O documento de dominio (secoes 3 a 5 e 17) descreve o fluxo, mas deixa duas coisas em aberto
que precisam ser decididas antes de escrever codigo:

1. **De onde vem o estado do pedido quando a decisao do antifraude chega.** A
   `FraudAnalysisCompleted` traz `orderId`, `customerId`, `decision`, `riskScore` e `reasons` --
   nao traz `totalAmount` nem `currency`. O `OrderProcessed` precisa deles.
2. **Como o pedido invalido sai do pipeline.** O documento so descreve o caminho feliz.

E ha uma restricao herdada da infraestrutura: o topico de decisao chega prefixado pelo
MirrorMaker 2 (`cluster-b.antifraud.analysis-completed`), e nada nesse nome pode estar em
codigo Java.

O requisito de forma mais forte deste change vem do usuario, nao do dominio: a classe de
topologia recebe apenas contratos do Kafka Streams e nao conhece nenhuma implementacao.

## Goals / Non-Goals

**Goals:**

- Cada etapa do pipeline e uma unidade propria, testavel isolada e nomeada na topologia.
- `OrdersTopology` legivel de cima a baixo: quem le a classe entende o fluxo sem abrir
  mais nenhum arquivo.
- Trocar a implementacao de qualquer etapa nao toca na topologia.
- Nenhum evento entra ou sai sem passar pela limpeza de headers.

**Non-Goals:**

- Calculo de risco. E o `antifraud` que decide; o `orders` so recebe a decisao.
- Reprocessamento automatico do que caiu na DLQ. A DLQ guarda o suficiente para
  reprocessar; quem reprocessa e um exercicio manual.
- `exactly_once_v2`. Change proprio.
- Consulta interativa (`queryableStoreName`) sobre o KTable de pedidos. Nao ha API HTTP no
  `orders`; o Kafka e a interface.

## Decisions

| # | Decisao | Alternativas descartadas | Conceito exercitado |
|---|---|---|---|
| D1 | **`orders.validated` e lido de volta como `KTable`** e cruzado com a decisao do antifraude por `orderId`. E dai que saem `totalAmount` e `currency` no `OrderProcessed`. | Repetir os campos dentro do `FraudAnalysisCompleted` -- inflaria o contrato entre dominios com dados que o antifraude nao usa, e o `orders` passaria a confiar no antifraude para devolver intacto um dado que e dele. | `KTable`, `KStream`⋈`KTable` join |
| D1a | **`PipelineContext` no lugar do `StreamsBuilder`.** O Kafka Streams aceita um unico source por topico, e `orders.validated` e lido por duas pipelines (a projecao e o estado do join). O contexto cria o source uma vez e o entrega as duas; a tabela sai do mesmo source via `toTable()`. | `builder.table()` numa pipeline e `builder.stream()` na outra -- a topologia e recusada com "has already been registered by another source". Derivar o fluxo de `table.toStream()` -- perde os headers, que a `KTable` nao guarda. | source compartilhado |
| D1b | **`OrderValidated` carrega `createdAt`.** O documento nao o lista na secao 4 mas o exige na secao 5; sem ele, a pipeline seguinte teria de reconstruir a data de um header auxiliar. | Header auxiliar `x-internal-order-created-at` -- foi a primeira versao, funcionava, e era a parte mais obscura do desenho: a data sumia do payload e reaparecia por um canal lateral. | — |
| D2 | **Nao existe classe de topologia.** O dominio declara uma `List<Pipeline>`, e `PipelineTopology` (no `shared`) constroi todas em ordem. Cada `Pipeline` le de topico, processa e escreve em topico; nenhuma sabe que as outras existem. | Uma `OrdersTopology` que monta o grafo inteiro -- foi a primeira versao, e o proprio desenho do grafo virou a parte dificil de ler. Um metodo privado por etapa dentro dela ainda deixava todas as etapas acopladas ao mesmo arquivo. | composicao |
| D2a | **Quatro formas de pipeline cobrem o dominio**: `ValuePipeline` (le, transforma sem mexer na chave, escreve), `ProcessorPipeline` (idem, podendo mudar a chave), `SplitPipeline` (uma entrada, varias saidas) e `JoinPipeline` (fluxo cruzado com tabela, varias saidas). Todas vivem no `shared` e servem aos dois dominios. | Uma classe por etapa concreta -- nada seria reaproveitado pelo antifraud. Uma unica pipeline configuravel por flags -- vira um interpretador. | — |
| D2b | **`OrdersStages` continua declarando cada etapa pelo contrato do Kafka Streams** que ela cumpre. A montagem em `OrdersPipelines` nao importa nenhuma implementacao -- checavel: os imports de `OrderValidationRules`, `OrdersMapper` e das classes `stage/` simplesmente nao existem naquele arquivo. | Passar as implementacoes direto -- trocar uma etapa viraria mudanca na montagem. | inversao de dependencia |
| D3 | **Validacao e um `Processor` que devolve `StageOutcome`,** e o roteamento acontece com `split()` na topologia. A etapa nao conhece a DLQ; a topologia nao conhece a regra. | Filtrar com `filter()` e reprocessar para descobrir o motivo -- valida duas vezes e perde a razao da rejeicao. Lancar excecao -- derruba a thread da task por causa de um pedido malformado. | `Processor` API, `split`/`branch` |
| D4 | **`FixedKeyProcessorSupplier` em toda etapa que nao muda a chave** -- projecao, limpeza de headers, carimbo de auxiliares. `Processor` completo apenas na validacao, que precisa emitir dois tipos de saida. | `Processor` em tudo -- o compilador deixaria de garantir que essas etapas nao reparticionam. | `FixedKeyProcessor` |
| D5 | **`split()` com `Branched.as` nomeando os ramos,** e `merge()` reunindo os dois para o evento final. Aprovado e rejeitado vao para topicos separados e ambos convergem em `orders.processed`. | Um so topico com campo `status` -- consumidor teria de filtrar e o desenho da secao 17 do documento se perderia. Nao ter `orders.processed` -- perde o evento que marca o fim do ciclo. | `split`/`branch`, `merge` |
| D6 | **A limpeza de headers e responsabilidade da propria saida.** Cada `Route` e cada pipeline aplicam `HeaderCleanup` antes do sink; quem monta nunca pede a limpeza. Acrescentar uma saida nova nao tem como esquece-la. | Limpar na montagem -- exatamente o que se esquece. Limpar dentro de cada etapa -- a etapa passaria a conhecer o sink. | — |
| D6a | **`Route` e interface, nao record.** Cada saida escreve um tipo diferente (`OrderValidated` num ramo, `DeadLetter` no outro) e uma `List` de records parametrizados pelo tipo de saida nao teria como ser declarada. Como interface, a rota sabe terminar a si mesma e a pipeline so precisa de `List<Route<K, V>>`. | Wildcard na lista -- nao compila no ponto de uso. Um `Object` e cast -- perde a verificacao que motivou o desenho. | generics |
| D7 | **Conversao entre eventos por MapStruct.** `OrderCreated`→`OrderValidated`, `OrderValidated`→`OrderForFraudAnalysis`, `OrderDecided`→`OrderProcessed`. | Builders a mao -- campo esquecido numa conversao vira `null` num topico sem que nada avise. | — |
| D8 | **Regras de validacao sao uma lista de `OrderValidationRule`,** cada uma devolvendo `Optional<String>` com o motivo. O processor soma os motivos; acrescentar regra nao altera o processor. | Uma cascata de `if` dentro do processor -- para no primeiro erro e esconde os demais, obrigando o produtor a corrigir um campo por vez. | — |
| D9 | **`totalAmount` e conferido contra a soma dos itens em `BigDecimal`,** pela classe `Money` do `shared`. | `double` -- a regra falharia de forma intermitente e so com certos valores. | — |
| D10 | **O `Main` e o unico lugar que conhece implementacoes.** Ele monta `OrdersStages` com um builder nomeado, campo a campo, e entrega a topologia ao `StreamsRunner`. | Um container de injecao -- Spring no modulo cuja razao de existir e nao ter Spring. Metodos estaticos de fabrica -- impede substituir uma etapa no teste. | — |

### A classe de topologia

```java
@RequiredArgsConstructor
public final class OrdersTopology implements TopologySupplier {

  private final OrdersTopics topics;
  private final OrdersSerdes serdes;
  private final OrdersStages stages;
  private final HeaderCleanup cleanup;

  @Override
  public Topology topology() { ... }
}
```

Nenhum dos quatro campos e um tipo de dominio. `OrdersTopics` sao nomes vindos da
configuracao; `OrdersSerdes` e `OrdersStages` sao records cujos campos sao tipos do Kafka
Streams; `HeaderCleanup` devolve um `FixedKeyProcessorSupplier`.

### Ordem das etapas e headers

Todo ramo termina em `cleanup` seguido de `to(...)`. A etapa de limpeza e a ultima antes de
cada um dos seis sinks -- inclusive o da DLQ, onde os auxiliares ja foram promovidos a
`x-error-*` pelo `DeadLetterMapper`.

## Risks / Trade-offs

- **O join `KStream`⋈`KTable` exige co-particionamento.** Se o `antifraud` publicar
  `FraudAnalysisCompleted` com chave diferente de `orderId`, o join silenciosamente nao
  encontra o par e nenhum `OrderProcessed` sai. → Fica como requisito explicito na spec do
  `pipeline-antifraud`, e um cenario de teste cobre o par ausente.
- **A decisao pode chegar antes do `OrderValidated` estar no KTable.** O `orders` publica os
  dois quase juntos, mas sao topicos diferentes. → O join `KStream`⋈`KTable` nao espera: sem
  par, o registro e descartado. Cobrir com um cenario que documenta o comportamento; a
  correcao (join com `KTable`-`KTable` ou janela de espera) fica para um change proprio se
  aparecer na pratica.
- **Seis sinks, seis limpezas.** Esquecer uma deixaria header interno vazar. → Um teste por
  topico de saida verificando ausencia do prefixo, e nao apenas um teste generico.
- **`OrderDecided` e `OrderProcessed` sao quase iguais.** → Sao momentos diferentes do ciclo
  (decidido x finalizado) e vao para topicos diferentes; fundi-los faria `orders.approved`
  carregar um `processedAt` que ainda nao aconteceu.

## Open Questions

- Se `orders.approved` e `orders.rejected` deveriam ser compactados por `orderId`. Comecam
  como topicos comuns; se o lab passar a consulta-los como estado, vira `cleanup.policy=compact`.
