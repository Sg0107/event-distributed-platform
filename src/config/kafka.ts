import { Kafka } from "kafkajs";

// Kafka configuration for event-platform-api
export const kafka = new Kafka({
  clientId: "event-platform-api",
  brokers: ["localhost:9092"],
});

export const MAX_RETRIES = 3;

export function getRetryDelay(retryCount: number): number {
  const delays: Record<number, number> = {
    1: 1000,
    2: 5000,
    3: 30000,
  };

  return delays[retryCount] ?? 0;
}
// Kafka producer for event-platform-api
export const producer = kafka.producer();
