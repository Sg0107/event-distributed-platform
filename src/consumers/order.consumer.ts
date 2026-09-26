import { kafka } from "../config/kafka.js"; // import the kafka instance
import { mongoDb } from "../config/mongodb.js";
import { redisClient } from "../config/redis.js";
import { publishToRetry } from "../producer/retry.producer.js";
const consumer = kafka.consumer({ 
    groupId: "order-service"
});
const WORKER_ID = process.env.WORKER_ID;
export async function startOrderConsumer() {

    await consumer.connect();
    console.log("kafka consumer connected");

    await consumer.subscribe({ topic: "orders", fromBeginning: true });
    console.log("kafka consumer subscribed to orders topic");

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log("Order worker received message from kafka consumer", {
                workerId: WORKER_ID,
                topic,
                partition,
                offset: message?.offset,
            });
            const value = message?.value?.toString();
            if (!value) {
                console.log("kafka consumer received message with no value");
                return;
            }
            let event; 
            try {
                event = JSON.parse(value);
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
                !event ||
                typeof event !== "object" ||
                typeof event.eventId !== "string" ||
                typeof event.eventType !== "string" ||
                typeof event.timestamp !== "string" ||
                !event.payload ||
                typeof event.payload !== "object" ||
                typeof event.payload.orderId !== "string" ||
                typeof event.payload.userId !== "string" ||
                typeof event.payload.amount !== "number"
            ) {
                console.error(
                  "Invalid order event format. Skipping malformed message.",
                  {
                    topic,
                    partition,
                    offset: message?.offset,
                  }
                );
              
                return;
            }
            console.log({"kafka consumer received message": {
                topic, partition, event, offset: message?.offset,
            }});
            try {
                const idempotencyKey = `processed:event:${event.eventId}`;

                const isProcessed = await redisClient.exists(idempotencyKey);

                if (isProcessed) {
                    console.log(`Skipping duplicate event ${event.eventId}`);
                    return;
                }

                // Temporary failure to simulate a failed event
                if(event.payload.orderId.startsWith("fail-")) {
                    throw new Error("Simulated order processing failure");
                }
                 
                // Success to simulate a retry event
                if (event.payload.orderId.startsWith("retry-success-")) {
                    throw new Error("Simulated initial processing failure");
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
                console.log(`Order ${event.payload.orderId} persisted to MongoDB and marked as processed`);
            } catch (error) {
                console.error(`Failed to process event ${event.eventId}:`, error);
                await publishToRetry(event, 1);
            }
        },
    });
}