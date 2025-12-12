import dotenv from 'dotenv';
import mongoose from 'mongoose';
import { Worker } from 'bullmq';
import crypto from 'crypto';
import { GetObjectCommand } from '@aws-sdk/client-s3';
import { PDFDocument } from 'pdf-lib';

import { OUTPUT_PDF_QUEUE_NAME, MERGE_PDF_QUEUE_NAME, connection } from '../queues/outputPdfQueue.js';
import { generateOutputPdfBuffer } from '../src/pdf/generateOutputPdf.js';
import { s3, uploadToS3 } from '../src/services/s3.js';
import Document from '../src/models/Document.js';
import DocumentAccess from '../src/models/DocumentAccess.js';
import DocumentJobs from '../src/models/DocumentJobs.js';
import User from '../src/models/User.js';

dotenv.config();

// ------------------------------
// Mongo + S3 helpers
// ------------------------------
async function connectMongo() {
  const mongoUri = process.env.MONGO_URI;
  if (!mongoUri) {
    console.error('MONGO_URI missing');
    process.exit(1);
  }
  await mongoose.connect(mongoUri);
  console.log('[pdfWorker] Connected to MongoDB');
}

async function ensureS3Env() {
  if (!process.env.AWS_S3_BUCKET) {
    console.warn('AWS_S3_BUCKET missing => uploads will fail');
  }
}

// ------------------------------
// Resolve S3 images
// ------------------------------
async function resolveS3ImagesToDataUrls(layoutPages) {
  const bucket = process.env.AWS_S3_BUCKET;
  const cache = new Map();
  const keysToFetch = new Set();

  for (const page of layoutPages || []) {
    for (const item of page.items || []) {
      if (item?.src?.startsWith('s3://')) {
        const key = item.src.slice(5);
        keysToFetch.add(key);
      }
    }
  }

  // Download images
  await Promise.all(
    [...keysToFetch].map(async (key) => {
      try {
        const command = new GetObjectCommand({ Bucket: bucket, Key: key });
        const res = await s3.send(command);

        const chunks = [];
        for await (const chunk of res.Body) chunks.push(chunk);
        const buffer = Buffer.concat(chunks);

        const contentType = res.ContentType || 'image/png';
        const dataUrl = `data:${contentType};base64,${buffer.toString('base64')}`;
        cache.set(key, dataUrl);
      } catch (err) {
        console.error('[pdfWorker] Failed to fetch image from S3', key, err);
      }
    })
  );

  // Replace S3 URLs with data URLs
  return (layoutPages || []).map((page) => ({
    ...page,
    items: page.items.map((item) => {
      if (item?.src?.startsWith('s3://')) {
        const key = item.src.slice(5);
        return { ...item, src: cache.get(key) || item.src };
      }
      return item;
    }),
  }));
}

// ------------------------------
// START WORKERS (NO CLUSTER)
// ------------------------------
async function startWorkers() {
  await connectMongo();
  await ensureS3Env();

  console.log('[pdfWorker] Starting SINGLE-PROCESS worker (Railway safe)...');

  // ------------------------------
  // Render Worker
  // ------------------------------
  new Worker(
    OUTPUT_PDF_QUEUE_NAME,
    async (job) => {
      const { email, assignedQuota, pageLayout, pageIndex, adminUserId, jobId } = job.data;

      const jobDoc = await DocumentJobs.findById(jobId);
      if (!jobDoc) return;

      jobDoc.status = 'processing';
      jobDoc.stage = 'rendering';
      await jobDoc.save();

      const user = await User.findOne({ email: email.toLowerCase() });
      if (!user) throw new Error(`User not found: ${email}`);

      const [resolvedPage] = await resolveS3ImagesToDataUrls([pageLayout]);
      const pdfBuffer = await generateOutputPdfBuffer([resolvedPage]);

      const { key } = await uploadToS3(pdfBuffer, 'application/pdf', 'generated/pages/');

      const updated = await DocumentJobs.findByIdAndUpdate(
        jobId,
        {
          $inc: { completedPages: 1 },
          $push: { pageArtifacts: { key, pageIndex } },
        },
        { new: true }
      );

      // When all pages are done → enqueue merge job
      if (updated.completedPages === updated.totalPages) {
        const { mergePdfQueue } = await import('../queues/outputPdfQueue.js');
        await mergePdfQueue.add('mergeJob', {
          jobId,
          email: email.toLowerCase(),
          assignedQuota,
          adminUserId,
        });

        updated.stage = 'merging';
        await updated.save();
      }

      console.log(`[pdfWorker] Rendered page ${pageIndex + 1}/${jobDoc.totalPages}`);
    },
    { connection, concurrency: 1 }
  );

  // ------------------------------
  // Merge Worker
  // ------------------------------
  new Worker(
    MERGE_PDF_QUEUE_NAME,
    async (job) => {
      const { jobId, email, assignedQuota, adminUserId } = job.data;

      const jobDoc = await DocumentJobs.findById(jobId);
      if (!jobDoc) return;

      jobDoc.stage = 'merging';
      jobDoc.status = 'processing';
      await jobDoc.save();

      const user = await User.findOne({ email: email.toLowerCase() });
      if (!user) throw new Error('User not found for merge');

      const bucket = process.env.AWS_S3_BUCKET;
      const sorted = [...jobDoc.pageArtifacts].sort((a, b) => a.pageIndex - b.pageIndex);

      const buffers = [];
      for (const artifact of sorted) {
        const cmd = new GetObjectCommand({ Bucket: bucket, Key: artifact.key });
        const res = await s3.send(cmd);

        const chunks = [];
        for await (const c of res.Body) chunks.push(c);
        buffers.push(Buffer.concat(chunks));
      }

      const mergedPdf = await PDFDocument.create();
      for (const bytes of buffers) {
        const src = await PDFDocument.load(bytes);
        const pages = await mergedPdf.copyPages(src, src.getPageIndices());
        pages.forEach((p) => mergedPdf.addPage(p));
      }

      const outBytes = await mergedPdf.save();
      const { key, url } = await uploadToS3(Buffer.from(outBytes), 'application/pdf', 'generated/output/');

      const doc = await Document.create({
        title: 'Generated Output',
        fileKey: key,
        fileUrl: url,
        mimeType: 'application/pdf',
        createdBy: adminUserId,
        documentType: 'generated-output',
      });

      await DocumentAccess.updateOne(
        { userId: user._id, documentId: doc._id },
        { userId: user._id, documentId: doc._id, assignedQuota, usedPrints: 0 },
        { upsert: true }
      );

      jobDoc.status = 'completed';
      jobDoc.stage = 'completed';
      jobDoc.outputDocumentId = doc._id;
      jobDoc.userId = user._id;
      await jobDoc.save();

      console.log(`[pdfWorker] Merge completed for job ${jobId}`);
    },
    { connection, concurrency: 1 }
  );

  console.log('[pdfWorker] Worker is LIVE and listening for jobs...');
}

// Start immediately (NO CLUSTER)
startWorkers().catch((err) => {
  console.error('[pdfWorker] Fatal:', err);
  process.exit(1);
});
