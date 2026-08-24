# Requisicoes HTTP do lab

Arquivos `.http` no formato do HTTP Client do IntelliJ (tambem lidos pela extensao REST Client do VS Code). Abra
qualquer um, escolha o environment no canto superior e clique na seta ao lado do `###`.

| arquivo | o que cobre |
|---|---|
| `health.http` | `GET /api/health` |
| `orders.http` | `POST /api/orders` -- pedido valido, caminhos de DLQ, rejeicao do antifraude, rajada |
| `config-server.http` | o que o config-server entrega para cada dominio |

## Environments

`http-client.env.json` traz dois:

- **local** -- `http://localhost:8090`. Use este. Vale para a API rodando no compose (a 8090 esta publicada) e para
  `go run ./cmd/api` no host.
- **docker** -- `http://api:8090`. So faz sentido de dentro da rede `cluster-a`, por exemplo num `docker exec`.

## Antes de disparar

```bash
docker compose up -d --wait
```

Isso sobe os dois clusters, o config-server (que cria os topicos no boot) e a API. As apps Streams rodam no host:

```bash
make orders       # cluster A
make antifraud    # cluster B
```

Sem o `orders` de pe, o `POST /api/orders` continua respondendo `202` -- a API so publica em `orders.created`. O
evento fica no topico esperando alguem consumir, que e o comportamento correto.

## O que 202 quer dizer aqui

`202 Accepted` significa que os brokers confirmaram a escrita (`RequiredAcks=all`, writer sincrono). Nao significa que
o pedido e valido: quem valida e o `ValidationPipeline` do modulo `orders`, e pedido invalido sai em `orders.dlq` com
o motivo nos headers. As unicas recusas do gateway sao `400` -- JSON malformado ou `orderId` ausente.

Para ver onde cada evento parou:

```bash
docker exec kafka-broker-a-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders.processed --from-beginning

docker exec kafka-broker-a-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders.dlq --from-beginning
```

Ou pelo kafbat em <http://localhost:8080>.
