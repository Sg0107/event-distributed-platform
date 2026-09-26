import { producer } from "./config/kafka.js";
import { connectToMongo } from "./config/mongodb.js";
import { connectToRedis } from "./config/redis.js";
import { startOrderConsumer } from "./consumers/order.consumer.js";

async function main() {
    await connectToMongo();
    await connectToRedis();
    await producer.connect();

    console.log(
        `Order worker ${process.env.WORKER_ID ?? "unknown"} starting`
    );

    await startOrderConsumer();
}

main().catch((error) => {
    console.error("Order worker failed:", error);
    process.exit(1);
});