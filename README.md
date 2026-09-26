# Event Distributed Platform

A small event-driven order platform. An HTTP API publishes `ORDER_CREATED` events to Kafka. Separate workers consume that stream to persist orders, send notifications, and record analytics.

## Architecture

```text
Order form / POST /orders
        |
        v
   API (Express) ----> Kafka topic: orders
                            |
            +---------------+---------------+
            |               |               |
            v               v               v
     Order worker    Notification     Analytics
            |          worker           worker
            v
     MongoDB (orders)
     Redis (idempotency)
            |
            | on failure
            v
   Kafka topic: order-retries
            |
            v
     Retry consumer (up to 3 attempts)
            |
            v
   Kafka topic: order-dlq
```

| Process | Entry file | Role |
| --- | --- | --- |
| API | `src/index.ts` | Connects to MongoDB, Redis, and the Kafka producer, then serves the order form and `POST /orders` |
| Order worker | `src/order-worker.ts` | Consumes `orders`, writes the order to MongoDB, and marks the event processed in Redis |
| Notification worker | `src/notification-worker.ts` | Consumes `orders`. It currently accepts only messages shaped like `{ event, retryCount }`, so a plain `ORDER_CREATED` event is skipped |
| Analytics worker | `src/analytics-worker.ts` | Consumes `orders` and logs the order for analytics |
| Retry consumer | `src/consumers/retry.consumer.ts` | Consumes `order-retries`, retries persistence, then publishes to `order-dlq` |

Each consumer uses its own Kafka group, so one published order is delivered to every worker.

## Topics

| Topic | Producer | Consumer |
| --- | --- | --- |
| `orders` | API | Order, notification, and analytics workers |
| `order-retries` | Order worker, retry consumer | Retry consumer |
| `order-dlq` | Retry consumer | None yet |

Messages on `orders` are keyed by `orderId`. The event body looks like this:

```json
{
  "eventId": "uuid",
  "eventType": "ORDER_CREATED",
  "timestamp": "2026-09-26T00:00:00.000Z",
  "payload": {
    "orderId": "order-001",
    "userId": "user-001",
    "amount": 1000
  }
}
```

## Prerequisites

- Node.js 20+
- Docker, for Kafka, Redis, and MongoDB

Local addresses are fixed in the config:

| Service | Address |
| --- | --- |
| Kafka | `localhost:9092` |
| Redis | `redis://localhost:6379` |
| MongoDB | `mongodb://localhost:27017` (database `event-platform`, collection `orders`) |
| API | `http://localhost:3000` |

## Run it

Start the infrastructure:

```bash
docker compose up -d
```

Install dependencies and start the API:

```bash
npm install
npm run dev
```

Open [http://localhost:3000](http://localhost:3000) and publish an order, or call the API directly:

```bash
curl -X POST http://localhost:3000/orders \
  -H "Content-Type: application/json" \
  -d '{"orderId":"order-001","userId":"user-001","amount":1000}'
```

`POST /orders` requires `orderId`, `userId`, and `amount`. A valid request returns `201` and the published event. Missing fields return `400`.

Start the workers in separate terminals:

```bash
WORKER_ID=order-1 npx tsx src/order-worker.ts
npx tsx src/notification-worker.ts
npx tsx src/analytics-worker.ts
```

`WORKER_ID` is optional. It is only printed in the order worker logs.

The retry consumer is implemented, but nothing starts it yet. Failed orders are still published to `order-retries`.

## Failure handling

The order worker skips an event when Redis already has `processed:event:<eventId>`. After a successful MongoDB insert it sets that key, so the same event is not written twice.

Two order id prefixes simulate failures:

| Order id prefix | Behavior |
| --- | --- |
| `fail-` | Processing throws. The event is retried, then sent to `order-dlq` after 3 attempts. |
| `retry-success-` | The first attempt throws. A later retry can persist the order. |

Retry delays are 1s, 5s, and 30s. `MAX_RETRIES` is 3.

## Scripts

`npm run dev` starts the API with `tsx` and reloads on file changes. `npm run build` and `npm start` are in `package.json`, but the repo has no `tsconfig.json`, so the compile step is not set up yet.
