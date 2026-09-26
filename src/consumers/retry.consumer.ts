import { kafka } from "../config/kafka";
import { mongoDb } from "../config/mongodb";
import { redisClient } from "../config/redis";
import { publishToDlq, publishToRetry } from "../producer/retry.producer";
import { MAX_RETRIES, getRetryDelay } from "../config/kafka";

const retryConsumer = kafka.consumer({
    groupId: "order-retry-service"
});


function sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function startRetryConsumer() {
    await retryConsumer.connect();
    console.log("kafka consumer connected");

    await retryConsumer.subscribe({ topic: "order-retries", fromBeginning: true });
    console.log("kafka consumer subscribed to order-retries topic");

    await retryConsumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const value = message?.value?.toString();

            if (!value) {
                console.log("kafka consumer received message with no value");
                return;
            }
            let retryMessage;
            try {
                retryMessage = JSON.parse(value);
            } catch (error) {
                console.error(
                    "Failed to parse Kafka message as JSON. Skipping malformed message.",
                    {
                        topic,
                        partition,
                        offset: message?.offset,
                        error,
                    }
                );
                return;
            }
            if (
                !retryMessage ||
                typeof retryMessage !== "object" ||
                !("event" in retryMessage) ||
                !("retryCount" in retryMessage)
              ) {
                console.error(
                  "Invalid retry message format. Skipping malformed message.",
                  {
                    topic,
                    partition,
                    offset: message.offset,
                  }
                );
              
                return;
            }
            const event = retryMessage.event;
            const retryCount = retryMessage.retryCount ?? 1;
            console.log(`Received retry message for event ${event.eventId}. Retry count: ${retryCount}`);

            try {
                const idempotencyKey = `processed:event:${event.eventId}`;
                const alreadyProcessed = await redisClient.exists(idempotencyKey);
                if (alreadyProcessed) {
                    console.log(`Skipping duplicate event ${event.eventId}`);
                    return;
                }

                const delay = getRetryDelay(retryCount); 

                if (delay > 0) {
                    console.log( `Waiting ${delay / 1000}s before retrying ` + `event ${event.eventId}` ); 
                    await sleep(delay); 
                }

                // Keep failing the order to simulate a temporary failure
                if(event.payload.orderId.startsWith("fail-")) {
                    throw new Error("Simulated order processing failure");
                }

                await mongoDb.collection("orders").insertOne({
                    eventId: event.eventId,
                    eventType: event.eventType,
                    timestamp: event.timestamp,
                    orderId: event.payload.orderId,
                    userId: event.payload.userId,
                    amount: event.payload.amount,
                    kafka: {
                        topic, partition, offset: message?.offset,
                    },
                    processedAt: new Date(),
                });
                await redisClient.set(idempotencyKey, "1");
                console.log(
                    `Retry succeeded for order ${event.payload.orderId}`
                  );
            } catch (error) {
                console.error(
                    `Retry attempt ${retryCount} failed for event ${event.eventId}`
                  );
                if (retryCount >= MAX_RETRIES) {
                    await publishToDlq(event, retryCount);
                    console.log(`Event ${event.eventId} reached maximum retries. Publishing to DLQ.`);
                    return;
                }
                await publishToRetry(event, retryCount + 1);
                console.log(`Event ${event.eventId} retried. Retry count: ${retryCount + 1}`);
            }
        },
    }).catch((error) => {
        console.error("Error in retry consumer:", error);
        process.exit(1);
    });
}