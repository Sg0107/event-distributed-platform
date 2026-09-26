import { MongoClient } from "mongodb";

const MONGO_URL = "mongodb://localhost:27017";
const dbName = "event-platform";

export const mongoClient = new MongoClient(MONGO_URL);
export const mongoDb = mongoClient.db(dbName);

export async function connectToMongo() {
    await mongoClient.connect();
    console.log("Connected to MongoDB");
}
