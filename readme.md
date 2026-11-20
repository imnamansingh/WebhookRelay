# WebhookRelay

WebhookRelay is a webhook processing example built with Node.js, Kafka, and MongoDB. An HTTP Producer API queues incoming webhooks on Kafka. A separate Consumer processes each message, routes failures through timed retry topics, and stores exhausted retries in MongoDB.

## Message flow

```text
HTTP client
    |
    v
Producer API
    |
    v
Kafka: webhooks --> Consumer
                       |  \
                       |   +--> MongoDB dead-letter collection
                       +------> webhooks-retry-10s
                                    |
                                    +--> webhooks-retry-60s
```

1. The Producer accepts a JSON request and publishes a message to `webhooks`.
2. The Consumer reads the message and applies the processing check using its `timeToPersist` value.
3. A failed attempt is queued on `webhooks-retry-10s`; another failed attempt is queued on `webhooks-retry-60s`.
4. The Consumer waits for each retry topic's configured delay before processing its messages.
5. After the retry attempts, the Consumer records the message and failure reason in MongoDB.

The processing check compares `timeToPersist` in seconds with the delay assigned to the message's topic. A webhook is processed when its requested time is within that delay. This provides payloads for exercising the success, retry, and dead-letter flows.

## Producer API

The Express API listens on port `3000` by default. Routes are mounted under `/api/v1/producer`:

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/api/v1/producer/webhook` | Publishes a webhook envelope to `webhooks`. |
| `POST` | `/api/v1/producer/failed_webhook` | Publishes a failed webhook to the retry topic specified by `topicName`. |

The envelope contains the request headers, JSON body, and receive timestamp. The `x-event-id` header is used as the Kafka message key.

A queued webhook returns HTTP `200` with `{"status":"queued"}`. Errors return HTTP `500` with a JSON body containing `status` and `statusText`.

Example request:

```sh
curl -X POST http://localhost:3000/api/v1/producer/webhook \
  -H "Content-Type: application/json" \
  -H "x-event-id: example-event-1" \
  -d '{"timeToPersist":0,"data":"example webhook"}'
```

## Kafka and Consumer

The Consumer uses the KafkaJS group `webhook-relay-consumer` and subscribes to these topics:

| Topic | Processing delay |
| --- | ---: |
| `webhooks` | 0 seconds |
| `webhooks-retry-10s` | 10 seconds |
| `webhooks-retry-60s` | 60 seconds |

Retry counts are carried in the `x-retry-count` Kafka record header. The Consumer uses message timestamps to enforce retry delays, and manages batch offsets explicitly.

Messages are JSON envelopes. For example:

```json
{
  "headers": {
    "x-event-id": "example-event-1"
  },
  "body": {
    "timeToPersist": 10,
    "data": "example"
  },
  "receivedAt": "2026-01-01T00:00:00.000Z"
}
```

## MongoDB dead-letter records

The Consumer connects to the `dead_letter_db` database. When retries are exhausted, it stores the source topic, partition, offset, message payload, failure reason, and timestamp in the dead-letter collection. Compose stores MongoDB files in the `mongodb_data` named volume.

## Run with Docker Compose

Docker Compose starts a three-node Kafka cluster, MongoDB, the Producer API, and the Consumer. Each application topic is created with three partitions and replication factor three before the Producer and Consumer start. The `test` service sends a continuous stream of sample webhooks.

Start the application services:

```sh
docker compose up --build -d kafka1 kafka2 kafka3 mongodb producer consumer
```

To start the sample webhook generator as well:

```sh
docker compose up --build -d
```

The Producer API is available at `http://localhost:3000`. To follow service logs:

```sh
docker compose logs -f producer consumer
```

Stop the services with:

```sh
docker compose down
```

## Run the Node.js services on the host

Create `producers/.env`:

```dotenv
PORT=3000
KAFKA_BROKER_URL=localhost:9092,localhost:9095,localhost:9096
```

Create `consumers/.env`:

```dotenv
KAFKA_BROKER_URL=localhost:9092,localhost:9095,localhost:9096
MONGODB_CONNECTION_URL=mongodb://localhost:27017
PRODUCER_URL=http://localhost:3000/api/v1/producer/failed_webhook
```

The Consumer appends `/dead_letter_db` to `MONGODB_CONNECTION_URL`. Host clients can bootstrap from `localhost:9092`, `localhost:9095`, and `localhost:9096`. Compose services use `kafka1:9093`, `kafka2:9093`, and `kafka3:9093`.

Start the Kafka cluster and MongoDB:

```sh
docker compose up -d kafka1 kafka2 kafka3 kafka-init mongodb
```

In separate terminals, install dependencies and start each service:

```sh
cd producers
npm install
node src/index.js
```

```sh
cd consumers
npm install
node src/consumer.js
```

Both Node.js services handle `SIGINT` and `SIGTERM` for graceful shutdown.

## Sample webhook generator

The script in `tests/test.js` cycles through payloads designed to exercise immediate processing, each retry delay, and dead-letter handling. `--count` sets the number of requests per batch, `--interval` sets the pause between batches in milliseconds, and `--url` selects the Producer endpoint.

```sh
node tests/test.js --count=40 --interval=250 --url=http://localhost:3000/api/v1/producer/webhook
```

The endpoint can also be supplied through the `PRODUCER_URL` environment variable. The script runs continuously and stops after five failed requests.

## Requirements

- Node.js 20.19.0 or newer for host-based runs.
- npm.
- Docker with Docker Compose.
