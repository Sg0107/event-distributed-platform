import { randomUUID } from "crypto";;

// Event interface for event-platform-api
export interface OrderCreatedEvent {
    eventId: string;
    eventType: "ORDER_CREATED";
    timestamp: string;

    payload: {
        orderId: string;
        userId: string;
        amount: number;
    }
}

export function createOrderCreatedEvent(
    orderId: string, userId: string, amount: number
): OrderCreatedEvent {
    return {
        eventId: randomUUID(),
        eventType: "ORDER_CREATED",
        timestamp: new Date().toISOString(),
        payload: {
            orderId,
            userId,
            amount,
        },
    };
}