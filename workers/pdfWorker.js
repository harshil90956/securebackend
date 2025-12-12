import dotenv from 'dotenv';
import mongoose from 'mongoose';
import { Worker } from 'bullmq';
import crypto from 'crypto';
import { GetObjectCommand } from '@aws-sdk/client-s3';
import { PDFDocument } from 'pdf-lib';
import cluster from 'cluster';
import os from 'os';

import { OUTPUT_PDF_QUEUE_NAME, MERGE_PDF_QUEUE_NAME, connection } from '../queues/outputPdfQueue.js';
import { generateOutputPdfBuffer } from '../src/pdf/generateOutputPdf.js';
import { s3, uploadToS3 } from '../src/services/s3.js';
import Document from '../src/models/Document.js';
import DocumentAccess from '../src/models/DocumentAccess.js';
import DocumentJobs from '../src/models/DocumentJobs.js';
import User from '../src/models/User.js';

dotenv.config();

async function connectMongo() {
  const mongoUri =
    process.env.MONGO_URI ||
    'mongodb+srv://gajeraakshit53_db_user:lvbGcIFW0ul5Bao6@akshit.thyfwea.mongodb.net/securepdf?retryWrites=true&w=majority';

  if (!mongoUri) {
    console.error('MONGO_URI is not set in environment');
    process.exit(1);
  }

  await mongoose.connect(mongoUri);
  console.log('[pdfWorker] Connected to MongoDB');
}

async function ensureS3Env() {
  if (!process.env.AWS_S3_BUCKET) {
    console.warn('[pdfWorker] AWS_S3_BUCKET is not set. Uploads will fail.');
  }
}

async function resolveS3ImagesToDataUrls(layoutPages) {
  const bucket = process.env.AWS_S3_BUCKET;

  if (!bucket) {
    throw new Error('AWS_S3_BUCKET is not configured for pdfWorker');
  }

  const cache = new Map(); // key -> dataUrl
  const keysToFetch = new Set();

  // First pass: collect all unique S3 keys we need
  for (const page of layoutPages || []) {
    for (const item of page.items || []) {
      if (item && typeof item.src === 'string' && item.src.startsWith('s3://')) {
        const key = item.src.slice('s3://'.length);
        if (!cache.has(key)) {
          keysToFetch.add(key);
        }
      }
    }
  }

  // Fetch all unique keys in parallel
  await Promise.all(
    Array.from(keysToFetch).map(async (key) => {
      try {
        const command = new GetObjectCommand({ Bucket: bucket, Key: key });
        const response = await s3.send(command);

        const chunks = [];
        for await (const chunk of response.Body) {
          chunks.push(chunk);
        }
        const buffer = Buffer.concat(chunks);

        const base64 = buffer.toString('base64');
        const contentType = response.ContentType || 'image/png';
        const dataUrl = `data:${contentType};base64,${base64}`;
        cache.set(key, dataUrl);
      } catch (err) {
        console.error('[pdfWorker] Failed to fetch image from S3 for key', key, err);
      }
    })
  );

  // Second pass: rebuild pages with resolved data URLs
  const pages = [];

  for (const page of layoutPages || []) {
    const newItems = [];
    for (const item of page.items || []) {
      if (item && typeof item.src === 'string' && item.src.startsWith('s3://')) {
        const key = item.src.slice('s3://'.length);
        const dataUrl = cache.get(key) || null;

        newItems.push({
          ...item,
          src: dataUrl || item.src,
        });
      } else {
        newItems.push(item);
      }
    }

    pages.push({
      ...page,
      items: newItems,
    });
  }

  return pages;
}

async function startWorkers(role = 'render-merge') {
  await connectMongo();
  await ensureS3Env();

  if (role === 'render' || role === 'render-merge') {
    // Worker 1: per-page rendering
    // eslint-disable-next-line no-new
    const renderWorker = new Worker(
      OUTPUT_PDF_QUEUE_NAME,
      async (job) => {
      const { email, assignedQuota, pageLayout, pageIndex, adminUserId, jobId } = job.data || {};

      const targetJobDoc = jobId ? await DocumentJobs.findById(jobId).catch(() => null) : null;

      if (!targetJobDoc) {
        console.warn('[pdfWorker] Page job has no corresponding DocumentJobs record', jobId);
        return;
      }

      targetJobDoc.status = 'processing';
      targetJobDoc.stage = 'rendering';
      await targetJobDoc.save();

      const user = await User.findOne({ email: email.toLowerCase() });
      if (!user) {
        targetJobDoc.status = 'failed';
        targetJobDoc.stage = 'failed';
        await targetJobDoc.save();
        throw new Error(`User with email ${email} not found`);
      }

      // Render a single page PDF
      const [pageWithDataUrl] = await resolveS3ImagesToDataUrls([pageLayout]);
      const pdfBuffer = await generateOutputPdfBuffer([pageWithDataUrl]);

      // Upload per-page PDF to S3
      const { key } = await uploadToS3(pdfBuffer, 'application/pdf', 'generated/pages/');

      // Update job with page artifact and progress
      const updated = await DocumentJobs.findByIdAndUpdate(
        jobId,
        {
          $inc: { completedPages: 1 },
          $push: { pageArtifacts: { key, pageIndex } },
          $set: { status: 'processing', stage: 'rendering', userId: user._id },
        },
        { new: true }
      );

      if (!updated) {
        console.warn('[pdfWorker] Failed to update job after page render', jobId);
        return;
      }

      // If all pages rendered, enqueue merge job
      if (updated.completedPages >= updated.totalPages && updated.totalPages > 0) {
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

      console.log(
        `[pdfWorker] Rendered page ${pageIndex + 1}/${targetJobDoc.totalPages} for job ${jobId}`
      );
    },
      {
        connection,
        // Each process handles one job at a time; scale with processes instead of concurrency
        concurrency: 1,
      }
    );

    renderWorker.on('failed', (job, err) => {
      console.error(
        '[pdfWorker] Render worker failed',
        job?.id,
        job?.data?.jobId,
        err
      );
    });
  }

  if (role === 'merge' || role === 'render-merge') {
    // Worker 2: merge final PDF
    // eslint-disable-next-line no-new
    const mergeWorker = new Worker(
      MERGE_PDF_QUEUE_NAME,
      async (job) => {
      const { jobId, email, assignedQuota, adminUserId } = job.data || {};

      const jobDoc = await DocumentJobs.findById(jobId).catch(() => null);
      if (!jobDoc) {
        console.warn('[pdfWorker] Merge job without DocumentJobs record', jobId);
        return;
      }

      jobDoc.stage = 'merging';
      jobDoc.status = 'processing';
      await jobDoc.save();

      const user = await User.findOne({ email: email.toLowerCase() });
      if (!user) {
        jobDoc.stage = 'failed';
        jobDoc.status = 'failed';
        await jobDoc.save();
        throw new Error(`User with email ${email} not found (merge)`);
      }

      const bucket = process.env.AWS_S3_BUCKET;
      if (!bucket) {
        throw new Error('AWS_S3_BUCKET is not configured for pdfWorker');
      }

      // Download all page PDFs and merge
      const sortedArtifacts = [...(jobDoc.pageArtifacts || [])].sort(
        (a, b) => a.pageIndex - b.pageIndex
      );

      const pdfDocs = [];
      for (const artifact of sortedArtifacts) {
        const command = new GetObjectCommand({ Bucket: bucket, Key: artifact.key });
        const response = await s3.send(command);
        const chunks = [];
        for await (const chunk of response.Body) {
          chunks.push(chunk);
        }
        const buffer = Buffer.concat(chunks);
        pdfDocs.push(buffer);
      }

      const mergedPdf = await PDFDocument.create();
      for (const pdfBytes of pdfDocs) {
        const src = await PDFDocument.load(pdfBytes);
        const copiedPages = await mergedPdf.copyPages(src, src.getPageIndices());
        copiedPages.forEach((p) => mergedPdf.addPage(p));
      }

      const mergedBytes = await mergedPdf.save();

      const { key, url } = await uploadToS3(
        Buffer.from(mergedBytes),
        'application/pdf',
        'generated/output/'
      );

      const doc = await Document.create({
        title: 'Generated Output',
        fileKey: key,
        fileUrl: url,
        totalPrints: 0,
        createdBy: adminUserId,
        mimeType: 'application/pdf',
        documentType: 'generated-output',
      });

      const parsedQuota = Number(assignedQuota);
      const access = await DocumentAccess.findOneAndUpdate(
        { userId: user._id, documentId: doc._id },
        { userId: user._id, documentId: doc._id, assignedQuota: parsedQuota, usedPrints: 0 },
        { upsert: true, new: true }
      );

      if (!access.sessionToken) {
        access.sessionToken = crypto.randomBytes(32).toString('hex');
        await access.save();
      }

      jobDoc.status = 'completed';
      jobDoc.stage = 'completed';
      jobDoc.outputDocumentId = doc._id;
      jobDoc.userId = user._id;
      await jobDoc.save();

      console.log(`[pdfWorker] Merge job ${jobId} completed for ${email}`);
    },
      {
        connection,
        concurrency: 1,
      }
    );

    mergeWorker.on('failed', (job, err) => {
      console.error(
        '[pdfWorker] Merge worker failed',
        job?.id,
        job?.data?.jobId,
        err
      );
    });
  }

  console.log(`[pdfWorker] ${role} worker started, listening for jobs...`);
}
if (cluster.isPrimary) {
  const renderWorkers = Number(process.env.RENDER_WORKERS || os.cpus().length);

  for (let i = 0; i < renderWorkers; i += 1) {
    cluster.fork({ WORKER_ROLE: 'render' });
  }

  // Single merge worker to avoid merge conflicts
  cluster.fork({ WORKER_ROLE: 'merge' });

  console.log(
    `[pdfWorker] Master started with ${renderWorkers} render workers and 1 merge worker`
  );
} else {
  const role = process.env.WORKER_ROLE || 'render-merge';
  startWorkers(role).catch((err) => {
    console.error('[pdfWorker] Fatal error in worker', err);
    process.exit(1);
  });
}
