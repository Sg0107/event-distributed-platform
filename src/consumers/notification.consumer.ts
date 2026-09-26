import {kafka} from "../config/kafka.js";

const consumer = kafka.consumer({groupId: "notification-consumer"});

async function deliverNotification(event: any): Promise<void> {
    console.log(
        `Notification delivered for order ${event.payload.orderId} to user ${event.payload.userId}`
    );
}

export async function startNotificationConsumer() {
    await consumer.connect();
    await consumer.subscribe({topic: "orders", fromBeginning: true});
    console.log("Notification consumer started and subscribed to orders topic");
    await consumer.run({
        eachMessage: async ({topic, partition, message}) => {
            const value = message.value?.toString();

            if (!value) {
                console.log("Notification consumer received a message with no value");
                return;
            }
            let event;
            try {
                event = JSON.parse(value);
            } catch (error) {
                console.error("Failed to parse Kafka message as JSON. Skipping malformed message.", {
                    topic,
                    partition,
                    offset: message.offset,
                    error,
                });
                return;
            }

            if (
                !event ||
                typeof event !== "object" ||
                !("event" in event) ||
                !("retryCount" in event)
            ) {
                console.error("Invalid event format. Skipping malformed message.", {
                    topic,
                    partition,
                    offset: message.offset,
                    event,
                });
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

            try {
                await deliverNotification(event);
            } catch (error) {
                console.error("Failed to deliver notification. Retrying...", {
                    topic,
                    partition,
                    offset: message.offset,
                    event,
                    error,
                });
            }
            
            console.log("Notification consumer received a message with value", {
                event,
                topic,
                partition,
                offset: message.offset,
            });
        }
    });
}

