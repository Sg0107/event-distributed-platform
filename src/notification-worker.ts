import { startNotificationConsumer } from "./consumers/notification.consumer.js";

async function main() {
    await startNotificationConsumer();
}

main().catch((err) => {
    console.error("Error starting notification consumer", err);
    process.exit(1);
});