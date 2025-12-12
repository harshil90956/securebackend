import dotenv from "dotenv";
import mongoose from "mongoose";
import { Worker } from "bullmq";
import crypto from "crypto";
import { GetObjectCommand } from "@aws-sdk/client-s3";
import { PDFDocument } from "pdf-lib";

import {
  OUTPUT_PDF_QUEUE_NAME,
  MERGE_PDF_QUEUE_NAME,
  connection,
} from "../queues/outputPdfQueue.js";

import { generateOutputPdfBuffer } from "../src/pdf/generateOutputPdf.js";
import { s3, uploadToS3 } from "../src/services/s3.js";

import Document from "../src/models/Document.js";
import DocumentAccess from "../src/models/DocumentAccess.js";
import DocumentJobs from "../src/models/DocumentJobs.js";
import User from "../src/models/User.js";

dotenv.config();

// -------------------------------------------------------
// ✅ CONNECT MONGODB SAFELY
// -------------------------------------------------------
async function connectMongo() {
  const uri = process.env.MONGO_URI;
  if (!uri) {
    console.error("❌ MONGO_URI missing");
    process.exit(1);
  }

  await mongoose.connect(uri);
  console.log("✅ [pdfWorker] MongoDB connected");
}

// -------------------------------------------------------
// ✅ RESOLVE S3 IMAGE URLs (used by render worker)
// -------------------------------------------------------
async function resolveS3ImagesToDataUrls(layoutPages) {
  const bucket = process.env.AWS_S3_BUCKET;
  if (!bucket) throw new Error("AWS_S3_BUCKET missing");

  const cache = new Map();
  const keysToFetch = new Set();

  for (const page of layoutPages) {
    for (const item of page.items || []) {
      if (item.src?.startsWith("s3://")) {
        const key = item.src.slice(5);
        if (!cache.has(key)) keysToFetch.add(key);
      }
    }
  }

  await Promise.all(
    [...keysToFetch].map(async (key) => {
      try {
        const response = await s3.send(
          new GetObjectCommand({ Bucket: bucket, Key: key })
        );

        const chunks = [];
        for await (const c of response.Body) chunks.push(c);

        const buffer = Buffer.concat(chunks);
        const base64 = buffer.toString("base64");
        const dataUrl = `data:image/png;base64,${base64}`;
        cache.set(key, dataUrl);
      } catch (e) {
        console.error("❌ Failed to fetch S3 image", key, e);
      }
    })
  );

  // Replace all s3://image with real data URLs
  return layoutPages.map((page) => ({
    ...page,
    items: page.items.map((item) => {
      if (item.src?.startsWith("s3://")) {
        const key = item.src.slice(5);
        return { ...item, src: cache.get(key) };
      }
      return item;
    }),
  }));
}

// -------------------------------------------------------
// 🚀 START WORKERS (Render + Merge)
// -------------------------------------------------------
async function startWorkers() {
  await connectMongo();

  // ---------------------------------------------------
  // 🎨 RENDER WORKER – fast, lightweight
  // ---------------------------------------------------
  new Worker(
    OUTPUT_PDF_QUEUE_NAME,
    async (job) => {
      const { email, pageLayout, pageIndex, adminUserId, assignedQuota, jobId } =
        job.data;

      const jobDoc = await DocumentJobs.findById(jobId);
      if (!jobDoc) return;

      jobDoc.status = "processing";
      jobDoc.stage = "rendering";
      await jobDoc.save();

      const user = await User.findOne({ email: email.toLowerCase() });

      // Convert S3 images → Data URLs
      const [pageWithResolvedImages] = await resolveS3ImagesToDataUrls([
        pageLayout,
      ]);

      // Create PDF page buffer
      const pdfBuffer = await generateOutputPdfBuffer([
        pageWithResolvedImages,
      ]);

      // Upload page to S3
      const { key } = await uploadToS3(
        pdfBuffer,
        "application/pdf",
        "generated/pages/"
      );

      await DocumentJobs.findByIdAndUpdate(jobId, {
        $inc: { completedPages: 1 },
        $push: { pageArtifacts: { key, pageIndex } },
      });

      const updated = await DocumentJobs.findById(jobId);

      // All pages done → send merge job
      if (updated.completedPages === updated.totalPages) {
        const { mergePdfQueue } = await import("../queues/outputPdfQueue.js");
        await mergePdfQueue.add("merge", {
          jobId,
          email,
          assignedQuota,
          adminUserId,
        });

        updated.stage = "merging";
        await updated.save();
      }

      console.log(
        `📝 Rendered page ${pageIndex + 1}/${jobDoc.totalPages} (${jobId})`
      );
    },
    { connection, concurrency: 1 }
  );

  // ---------------------------------------------------
  // 🔥 MERGE WORKER – optimized + super fast
  // ---------------------------------------------------
  new Worker(
    MERGE_PDF_QUEUE_NAME,
    async (job) => {
      const { jobId, email, assignedQuota, adminUserId } = job.data;

      const jobDoc = await DocumentJobs.findById(jobId);
      if (!jobDoc) return;

      const bucket = process.env.AWS_S3_BUCKET;

      const sorted = [...jobDoc.pageArtifacts].sort(
        (a, b) => a.pageIndex - b.pageIndex
      );

      console.log(`⚡ Merging ${sorted.length} pages...`);
      console.time("DOWNLOAD_PAGES");

      // 🚀 Download ALL PDFs in parallel
      const pdfBuffers = await Promise.all(
        sorted.map(async (artifact) => {
          const response = await s3.send(
            new GetObjectCommand({ Bucket: bucket, Key: artifact.key })
          );
          const chunks = [];
          for await (const c of response.Body) chunks.push(c);
          return Buffer.concat(chunks);
        })
      );

      console.timeEnd("DOWNLOAD_PAGES");

      // 🧠 Merge PDF buffers
      const mergedPdf = await PDFDocument.create();

      console.time("MERGE_PAGES");

      for (const pdfBytes of pdfBuffers) {
        const src = await PDFDocument.load(pdfBytes, {
          ignoreEncryption: true,
          updateMetadata: false,
        });

        const copied = await mergedPdf.copyPages(
          src,
          src.getPageIndices()
        );

        for (const page of copied) mergedPdf.addPage(page);
      }

      const mergedBytes = await mergedPdf.save();

      console.timeEnd("MERGE_PAGES");

      // Upload final merged PDF
      const { key, url } = await uploadToS3(
        Buffer.from(mergedBytes),
        "application/pdf",
        "generated/output/"
      );

      const doc = await Document.create({
        title: "Generated Output",
        fileKey: key,
        fileUrl: url,
        documentType: "generated-output",
        createdBy: adminUserId,
      });

      // Create Document Access
      const access = await DocumentAccess.findOneAndUpdate(
        { userId: jobDoc.userId, documentId: doc._id },
        { assignedQuota, usedPrints: 0 },
        { upsert: true, new: true }
      );

      if (!access.sessionToken)
        access.sessionToken = crypto.randomBytes(32).toString("hex");

      await access.save();

      jobDoc.status = "completed";
      jobDoc.stage = "completed";
      jobDoc.outputDocumentId = doc._id;
      await jobDoc.save();

      console.log(`✅ Merge Completed for job ${jobId}`);
    },
    { connection, concurrency: 1 }
  );

  console.log("🚀 pdfWorker running (render + merge) in single-process mode");
}

// -------------------------------------------------------
startWorkers().catch((err) => {
  console.error("❌ Worker crashed", err);
  process.exit(1);
});
