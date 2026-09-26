import {producer} from "../config/kafka";

const RETRY_TOPIC = "order-retries";
const DLQ_TOPIC = "order-dlq";

export interface RetryMessage {
    event: unknown;
    retryCount: number;
}

export async function publishToRetry(event: unknown, retryCount: number) {
    const message: RetryMessage = {
        event,
        retryCount,
    };

    await producer.send({
        topic: RETRY_TOPIC,
        messages: [{ value: JSON.stringify(message) }],
    });

    console.log(
        `Event published to retry topic. retryCount=${retryCount}`
      );
}

export async function publishToDlq(event: unknown, retryCount: number) {

    const message: RetryMessage = {
        event,
        retryCount,
    };

    await producer.send({
        topic: DLQ_TOPIC,
        messages: [{ value: JSON.stringify(message) }],
    });

    console.log(
        `Event published to DLQ. retryCount=${retryCount}`
      );
}