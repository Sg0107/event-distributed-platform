import { kafka } from "../config/kafka.js";

const consumer = kafka.consumer({ groupId: "analytics-consumer" });


export async function startAnalyticsConsumer() {
    await consumer.connect();
    console.log("Kafka consumer connected");
    await consumer.subscribe({ topic: "orders", fromBeginning: true });
    console.log("analytics consumer subscribed to orders topic");
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const value = message?.value?.toString();
            if (!value) {
                console.log("kafka consumer received message with no value");
                return;
            }
            let event;
            try {
                event = JSON.parse(value);
            } catch (error) {
                console.error("Failed to parse event", error);
                return;
            }
            if (!event?.payload?.orderId || !event?.payload?.userId) {
                console.error("Invalid event format. Skipping malformed message.", {
                    topic,
                    partition,
                    offset: message.offset,
                    event,
                });
                return;
            }
            console.log("Analytics event processed", {
                orderId: event.payload.orderId,
                userId: event.payload.userId,
                amount: event.payload.amount,
                eventId: event.eventId,
                partition,
                offset: message.offset,
            });
        }
    });
}