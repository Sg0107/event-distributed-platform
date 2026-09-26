export interface RetryMetadata {
    retryCount: number;
    originalTopic: string;
    originalPartition: number;
    originalOffset: string;
}

export const MAX_RETRIES = 3;

