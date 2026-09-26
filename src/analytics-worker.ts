import { startAnalyticsConsumer } from "./consumers/analytics.consumer.js";

async function main() {
    await startAnalyticsConsumer();
}

main().catch((err) => {
    console.error("Error starting analytics consumer", err);
    process.exit(1);
});