-- Exemplo minimo: le orders.created do cluster A e agrega por cliente.
--
--   make flink-example
--
-- Roda ate o fim de proposito: 'scan.bounded.mode' = 'latest-offset' faz a
-- fonte parar no ultimo offset do momento em que o job comeca. Sem isso a query
-- fica de pe para sempre (que e o caso normal do Flink) e o make nunca retorna.
-- Para o modo continuo de verdade, use "make flink-sql" e omita essa opcao.

SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';

CREATE TABLE IF NOT EXISTS orders_created (
  orderId  STRING,
  customer STRING,
  amount   DOUBLE,
  -- Metadados do registro Kafka, expostos como coluna. E o que responde
  -- "esse evento caiu em qual particao?" sem sair para o console consumer.
  `partition` BIGINT METADATA VIRTUAL,
  `offset`    BIGINT METADATA VIRTUAL,
  event_time  TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders.created',
  -- Os tres brokers, nao um: o mesmo motivo do README. Endereco INTERNAL,
  -- porque quem conecta e um container, nao o host.
  'properties.bootstrap.servers' = 'kafka-broker-a-1:9092,kafka-broker-a-2:9092,kafka-broker-a-3:9092',
  'properties.group.id' = 'flink-sql-lab',
  'scan.startup.mode' = 'earliest-offset',
  'scan.bounded.mode' = 'latest-offset',
  'format' = 'json',
  -- Lab: uma mensagem malformada nao deve derrubar a query inteira.
  'json.ignore-parse-errors' = 'true'
);

SELECT
  customer,
  COUNT(*)     AS pedidos,
  SUM(amount)  AS total
FROM orders_created
GROUP BY customer;
