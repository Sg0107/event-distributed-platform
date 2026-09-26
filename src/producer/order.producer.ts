import { producer } from "../config/kafka";
import { OrderCreatedEvent, createOrderCreatedEvent } from "../kafka/events";

const ORDERS_TOPIC = "orders";

export async function publishOrderCreatedEvent(
    orderId: string, userId: string, amount: number
    // what this Promise<OrderCreatedEvent> is doing
    // is that it is returning a promise that resolves to an OrderCreatedEvent
    // this is useful because we can await the promise to get the event
    // and we can use the event in the rest of the code
    // this is a good practice because it allows us to handle errors gracefully
    // and it makes the code more readable
): Promise<OrderCreatedEvent> {
    const event = createOrderCreatedEvent(orderId, userId, amount);
    await producer.send({
        topic: ORDERS_TOPIC, // orders topic is the topic for the event
        messages: [{
            key: orderId, // orderId is the key for the event used for partitions
            value: JSON.stringify(event), // event is the value of the event
        }],
    });
    return event;
}
