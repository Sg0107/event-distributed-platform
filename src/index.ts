import { app } from "./api/server";
import { producer } from "./config/kafka";
import { connectToMongo } from "./config/mongodb";
import { connectToRedis } from "./config/redis";

const PORT = 3000;

async function main() {
    // start the mongoDB connection
    await connectToMongo();
    console.log("mongodb connected");

    // start the redis connection
    await connectToRedis();
    console.log("redis connected completed");

    // connect to the producer
    await producer.connect();
    console.log("kafka producer connected");

    // start the server
    app.listen(PORT, () => {
        console.log(`event-platform-api is running on http://localhost:${PORT}`);
    });
}

main().catch((error) => {
    console.error("Failed to start application:", error);
    process.exit(1);
});