import express from "express";
import { publishOrderCreatedEvent } from "../producer/order.producer";
import path from "path";
import { fileURLToPath } from "url";


// create a new express app
export const app = express();
app.use(express.json());

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

app.use(express.static(path.join(__dirname, "../public")));

app.post("/orders", async (req, res) => {
    try {
        const { orderId, userId, amount } = req.body ?? {};
        if (!orderId || !userId || !amount) {
            // using 400 status code because it is a bad request and the client should know that they need to send the required fields
            return res.status(400).json({ error: "Missing required fields" });
        }
        const event = await publishOrderCreatedEvent(orderId, userId, amount);
        // why we are not using 200 status code?
        // because we are creating a new resource and we want to return the created resource
        return res.status(201).json({ message: "Order created successfully", event });
    } catch (error) {
        console.error("Error in publishing order created event", error);
        // using 500 status code because it is a server error and the client should know that the server failed to process the request
        return res.status(500).json({ error: "Failed to publish order created event" });
    }
});