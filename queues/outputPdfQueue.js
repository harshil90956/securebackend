import { Queue } from 'bullmq';
import dotenv from 'dotenv';

dotenv.config();

// Prefer a full Redis URL (e.g. Upstash rediss://...) if provided
const redisUrl = process.env.REDIS_URL || process.env.UPSTASH_REDIS_URL;

export const connection = redisUrl
  ? { url: redisUrl }
  : {
      host: process.env.REDIS_HOST || '127.0.0.1',
      port: Number(process.env.REDIS_PORT || 6379),
    };

// Debug log to verify which Redis connection is being used
// This will print once when the backend starts.
// eslint-disable-next-line no-console
console.log('[queues/outputPdfQueue] Using Redis connection:', connection);

export const OUTPUT_PDF_QUEUE_NAME = 'outputPdfQueue';
export const MERGE_PDF_QUEUE_NAME = 'mergePdfQueue';

export const outputPdfQueue = new Queue(OUTPUT_PDF_QUEUE_NAME, {
  connection,
});

export const mergePdfQueue = new Queue(MERGE_PDF_QUEUE_NAME, {
  connection,
});
