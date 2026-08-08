# Kafka Streams Lab

Laboratório para estudo de **Kafka Streams puro**, utilizando dois domínios independentes que se comunicam exclusivamente por eventos Kafka:

- `orders`
- `antifraud`

O objetivo é explorar processamento **stateless e stateful**, agregações, `KStream`, `KTable`, windowing, joins, state stores, repartitioning e processamento orientado a eventos.

---

# 1. Fluxo das Streams

## Domínio: Orders

```text
OrderCreated
     │
     ▼
OrderValidated
     │
     ▼
OrderForFraudAnalysis
     │
     │
     └──────────────────────────┐
                                │
                                ▼
                         [ Kafka Topic ]
                                │
                                ▼
                     ┌───────────────────┐
                     │     Antifraud     │
                     └───────────────────┘
                                │
                                ▼
                     FraudAnalysisCompleted
                                │
                                │
     ┌──────────────────────────┘
     ▼
OrderApproved / OrderRejected
     │
     ▼
OrderProcessed
```

## Domínio: Antifraud

```text
OrderForFraudAnalysis
          │
          ▼
    Análise de risco
          │
          ├── Histórico do cliente
          ├── Quantidade de pedidos
          ├── Valor dos pedidos
          ├── Frequência dos pedidos
          └── Regras de fraude
          │
          ▼
FraudAnalysisCompleted
          │
          ├── APPROVED
          └── REJECTED
```

## Comunicação entre os domínios

```text
┌─────────────────────────────┐
│           ORDERS            │
│                             │
│ OrderCreated                │
│      ↓                      │
│ OrderValidated              │
│      ↓                      │
│ OrderForFraudAnalysis      │
└──────────────┬──────────────┘
               │
               │ Kafka
               ▼
┌─────────────────────────────┐
│          ANTIFRAUD          │
│                             │
│ OrderForFraudAnalysis       │
│      ↓                      │
│ Risk Analysis               │
│      ↓                      │
│ FraudAnalysisCompleted      │
└──────────────┬──────────────┘
               │
               │ Kafka
               ▼
┌─────────────────────────────┐
│           ORDERS            │
│                             │
│ FraudAnalysisCompleted      │
│      ↓                      │
│ OrderApproved / Rejected    │
│      ↓                      │
│ OrderProcessed              │
└─────────────────────────────┘
```

---

# 2. Domínio Orders

O domínio `orders` é responsável pelo ciclo de vida do pedido.

Ele **não decide se um pedido é fraudulento**.

Sua responsabilidade é:

- receber pedidos;
- validar os dados;
- solicitar análise antifraude;
- receber a decisão do antifraude;
- atualizar o estado do pedido;
- produzir o resultado final.

---

# 3. Evento: OrderCreated

Representa a criação de um novo pedido.

```json
{
  "orderId": "ord-001",
  "customerId": "cus-001",
  "items": [
    {
      "productId": "prod-001",
      "quantity": 2,
      "unitPrice": 150.00
    }
  ],
  "totalAmount": 300.00,
  "currency": "BRL",
  "createdAt": "2026-08-07T18:30:00Z"
}
```

## Regras

O evento deve possuir:

- `orderId` obrigatório;
- `customerId` obrigatório;
- pelo menos um item;
- `productId` obrigatório;
- `quantity` maior que zero;
- `unitPrice` maior que zero;
- `totalAmount` maior que zero;
- `currency` obrigatória;
- `createdAt` obrigatório.

O `totalAmount` deve ser igual à soma:

```text
quantity × unitPrice
```

de todos os itens.

---

# 4. Evento: OrderValidated

Após a validação, o domínio `orders` produz:

```json
{
  "orderId": "ord-001",
  "customerId": "cus-001",
  "totalAmount": 300.00,
  "currency": "BRL",
  "validatedAt": "2026-08-07T18:30:01Z"
}
```

Esse evento representa:

> "O pedido possui informações válidas e pode seguir para análise."

---

# 5. Evento: OrderForFraudAnalysis

Esse é o contrato utilizado para comunicação entre os dois domínios.

```json
{
  "orderId": "ord-001",
  "customerId": "cus-001",
  "totalAmount": 300.00,
  "currency": "BRL",
  "createdAt": "2026-08-07T18:30:00Z"
}
```

O `orders` não precisa conhecer a implementação do antifraude.

Ele simplesmente publica o evento.

---

# 6. Evento: FraudAnalysisCompleted

Produzido pelo domínio `antifraud`.

```json
{
  "orderId": "ord-001",
  "customerId": "cus-001",
  "decision": "APPROVED",
  "riskScore": 12,
  "reasons": [],
  "analyzedAt": "2026-08-07T18:30:02Z"
}
```

Um exemplo de pedido rejeitado:

```json
{
  "orderId": "ord-002",
  "customerId": "cus-001",
  "decision": "REJECTED",
  "riskScore": 90,
  "reasons": [
    "TOO_MANY_ORDERS_IN_SHORT_PERIOD",
    "AMOUNT_ABOVE_CUSTOMER_PROFILE"
  ],
  "analyzedAt": "2026-08-07T18:30:02Z"
}
```

---

# 7. Domínio Antifraud

O `antifraud` recebe pedidos e calcula um **risco fictício**.

Não é necessário implementar machine learning.

O objetivo é criar um conjunto de regras suficientemente interessante para explorar Kafka Streams.

O antifraude deve manter uma visão do comportamento de cada cliente.

Exemplo:

```text
Customer: cus-001

Pedidos:
  R$ 100
  R$ 150
  R$ 120
  R$ 180

Média: R$ 137,50
```

Quando chega:

```text
Novo pedido: R$ 2.000
```

o antifraude pode considerar esse comportamento suspeito.

---

# 8. Risk Score

O antifraude calcula um `riskScore` entre `0` e `100`.

Inicialmente:

```text
0 - 29   → baixo risco
30 - 69  → risco moderado
70 - 100 → alto risco
```

A decisão final é:

```text
riskScore < 70
    → APPROVED

riskScore >= 70
    → REJECTED
```

---

# 9. Regras de Antifraude

## Regra 1 — Valor elevado

Se o pedido possuir valor superior a **R$ 2.000**:

```text
+30 pontos
```

Exemplo:

```text
Pedido: R$ 2.500

Risk score:
0 + 30 = 30
```

---

## Regra 2 — Muitos pedidos em curto período

Se o cliente realizar **mais de 3 pedidos dentro de 1 minuto**:

```text
+40 pontos
```

Exemplo:

```text
10:00:01 → R$ 100
10:00:15 → R$ 150
10:00:30 → R$ 200
10:00:45 → R$ 300
```

O quarto pedido ultrapassa o limite.

Resultado:

```text
+40 pontos
```

Essa regra deve utilizar uma **window temporal**.

---

## Regra 3 — Valor muito acima do histórico

O antifraude deve calcular o valor médio dos pedidos anteriores do cliente.

Se:

```text
valor do novo pedido > média histórica × 5
```

adicionar:

```text
+30 pontos
```

Exemplo:

```text
Histórico:

R$ 100
R$ 120
R$ 150
R$ 130

Média = R$ 125

Novo pedido = R$ 700

700 > 125 × 5

700 > 625

→ regra acionada

+30 pontos
```

---

# 10. Combinação das regras

As regras são cumulativas.

Exemplo:

```text
Pedido:
R$ 3.000

Cliente:
média histórica = R$ 200

Últimos 60 segundos:
4 pedidos
```

Aplicação:

```text
Valor > R$ 2.000
→ +30

Mais de 3 pedidos/minuto
→ +40

Valor > média × 5
→ +30
```

Resultado:

```text
30 + 40 + 30 = 100
```

Como o limite é `70`:

```text
riskScore = 100
decision = REJECTED
```

---

# 11. Limite do Risk Score

O score nunca deve ultrapassar `100`.

Portanto:

```text
riskScore = min(score, 100)
```

Exemplo:

```text
30 + 40 + 30 + 20 = 120

resultado:

100
```

---

# 12. Reasons

Cada regra acionada deve adicionar uma razão ao evento.

Exemplo:

```json
{
  "decision": "REJECTED",
  "riskScore": 100,
  "reasons": [
    "HIGH_ORDER_AMOUNT",
    "TOO_MANY_ORDERS_IN_SHORT_PERIOD",
    "AMOUNT_ABOVE_CUSTOMER_PROFILE"
  ]
}
```

As razões devem ser determinísticas.

Sugestão de enum:

```text
HIGH_ORDER_AMOUNT
TOO_MANY_ORDERS_IN_SHORT_PERIOD
AMOUNT_ABOVE_CUSTOMER_PROFILE
```

---

# 13. Regra de idempotência

O mesmo `orderId` não deve ser analisado duas vezes como se fossem dois pedidos diferentes.

Se:

```text
OrderForFraudAnalysis
orderId = ord-001
```

for recebido novamente, o antifraude deve reconhecer que aquele pedido já foi processado.

Essa regra é importante para estudar o comportamento de processamento de eventos e reprocessamento.

---

# 14. Regra de cliente sem histórico

Se o cliente nunca realizou um pedido anteriormente:

```text
historicalAverage = undefined
```

A regra:

```text
AMOUNT_ABOVE_CUSTOMER_PROFILE
```

não deve ser aplicada.

O cliente novo deve ser analisado apenas pelas demais regras.

---

# 15. Regra de histórico

O antifraude deve manter, por cliente:

```text
customerId
totalOrders
totalAmount
averageOrderAmount
lastOrderAt
```

Exemplo:

```text
Customer: cus-001

totalOrders: 10
totalAmount: 1850.00
averageOrderAmount: 185.00
lastOrderAt: 2026-08-07T18:30:00Z
```

Essa informação representa o estado atual conhecido pelo antifraude.

---

# 16. Resultado do Antifraude

Depois de analisar o pedido, o antifraude produz:

```text
FraudAnalysisCompleted
```

Com:

```text
orderId
customerId
decision
riskScore
reasons
analyzedAt
```

Exemplo aprovado:

```json
{
  "orderId": "ord-001",
  "customerId": "cus-001",
  "decision": "APPROVED",
  "riskScore": 0,
  "reasons": [],
  "analyzedAt": "2026-08-07T18:30:02Z"
}
```

Exemplo rejeitado:

```json
{
  "orderId": "ord-002",
  "customerId": "cus-001",
  "decision": "REJECTED",
  "riskScore": 100,
  "reasons": [
    "HIGH_ORDER_AMOUNT",
    "TOO_MANY_ORDERS_IN_SHORT_PERIOD",
    "AMOUNT_ABOVE_CUSTOMER_PROFILE"
  ],
  "analyzedAt": "2026-08-07T18:31:02Z"
}
```

---

# 17. Fluxo final de negócio

```text
                    ORDER
                      │
                      ▼
               OrderCreated
                      │
                      ▼
              OrderValidated
                      │
                      ▼
          OrderForFraudAnalysis
                      │
                      │
                      ▼
              ┌──────────────┐
              │  ANTIFRAUD   │
              │              │
              │ Histórico    │
              │ Window       │
              │ Risk Score   │
              │ Rules        │
              └──────┬───────┘
                     │
                     ▼
          FraudAnalysisCompleted
                     │
                     ▼
              ┌──────────────┐
              │    ORDERS    │
              └──────┬───────┘
                     │
             ┌───────┴────────┐
             ▼                ▼
          APPROVED          REJECTED
             │                │
             ▼                ▼
       OrderProcessed    OrderRejected
```

---

# 18. Evolução técnica esperada

A implementação pode evoluir nesta ordem:

```text
1. KStream
      ↓
2. filter / map / mapValues
      ↓
3. groupByKey
      ↓
4. KTable
      ↓
5. aggregate
      ↓
6. State Store
      ↓
7. Time Window
      ↓
8. KStream/KTable Join
      ↓
9. Repartitioning
      ↓
10. Exactly-Once
```

A ideia é que **cada nova regra de negócio force a introdução de um conceito diferente do Kafka Streams**, em vez de criar complexidade artificial.