import express from 'express';
import multer from 'multer';
import crypto from 'crypto';
import Document from '../models/Document.js';
import DocumentAccess from '../models/DocumentAccess.js';
import DocumentJobs from '../models/DocumentJobs.js';
import { uploadToS3, s3 } from '../services/s3.js';
import { GetObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { authMiddleware } from '../middleware/auth.js';
import { mergePdfQueue } from '../../queues/outputPdfQueue.js';

const router = express.Router();
const upload = multer();

// Helper to generate opaque session tokens
const generateSessionToken = () => crypto.randomBytes(32).toString('hex');

// Upload document (PDF/SVG) for the logged-in user and create access record
router.post('/upload', authMiddleware, upload.single('file'), async (req, res) => {
  try {
    const { title, totalPrints } = req.body;
    const file = req.file;

    if (!file) {
      return res.status(400).json({ message: 'File is required' });
    }

    if (!title || !totalPrints) {
      return res.status(400).json({ message: 'Title and totalPrints are required' });
    }

    const parsedTotal = Number(totalPrints);
    if (Number.isNaN(parsedTotal) || parsedTotal <= 0) {
      return res.status(400).json({ message: 'totalPrints must be a positive number' });
    }

    const { key, url } = await uploadToS3(file.buffer, file.mimetype, 'securepdf/');

    const doc = await Document.create({
      title,
      fileKey: key,
      fileUrl: url,
      totalPrints: parsedTotal,
      createdBy: req.user._id,
    });

    const sessionToken = generateSessionToken();

    const access = await DocumentAccess.create({
      userId: req.user._id,
      documentId: doc._id,
      assignedQuota: parsedTotal,
      usedPrints: 0,
      sessionToken,
    });

    const loweredName = title.toLowerCase();
    const isSvg = file.mimetype === 'image/svg+xml' || loweredName.endsWith('.svg');
    const documentType = isSvg ? 'svg' : 'pdf';

    return res.status(201).json({
      sessionToken,
      documentTitle: doc.title,
      documentId: doc._id,
      remainingPrints: access.assignedQuota - access.usedPrints,
      maxPrints: access.assignedQuota,
      documentType,
    });
  } catch (err) {
    console.error('Docs upload error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

// Secure render: stream PDF/SVG bytes based on session token
router.post('/secure-render', authMiddleware, async (req, res) => {
  try {
    const { sessionToken } = req.body;

    if (!sessionToken) {
      return res.status(400).json({ message: 'sessionToken is required' });
    }

    const access = await DocumentAccess.findOne({ sessionToken }).populate('documentId');
    if (!access) {
      return res.status(404).json({ message: 'Access not found' });
    }

    if (access.userId.toString() !== req.user._id.toString()) {
      return res.status(403).json({ message: 'Not authorized for this document' });
    }

    const doc = access.documentId;
    if (!doc) {
      return res.status(404).json({ message: 'Document not found' });
    }

    const bucket = process.env.AWS_S3_BUCKET;
    if (!bucket) {
      return res.status(500).json({ message: 'S3 not configured' });
    }

    const command = new GetObjectCommand({
      Bucket: bucket,
      Key: doc.fileKey,
    });

    const s3Response = await s3.send(command);

    const chunks = [];
    for await (const chunk of s3Response.Body) {
      chunks.push(chunk);
    }
    const buffer = Buffer.concat(chunks);

    const loweredTitle = (doc.title || '').toLowerCase();
    const isSvg = loweredTitle.endsWith('.svg');

    res.setHeader('Content-Type', isSvg ? 'image/svg+xml' : 'application/pdf');
    return res.send(buffer);
  } catch (err) {
    console.error('Secure render error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

// Secure print: decrement quota and return presigned S3 URL for printing
router.post('/secure-print', authMiddleware, async (req, res) => {
  try {
    const { sessionToken } = req.body;

    if (!sessionToken) {
      return res.status(400).json({ message: 'sessionToken is required' });
    }

    const access = await DocumentAccess.findOne({ sessionToken }).populate('documentId');
    if (!access) {
      return res.status(404).json({ message: 'Access not found' });
    }

    const remaining = access.assignedQuota - access.usedPrints;
    if (remaining <= 0) {
      return res.status(400).json({ message: 'Print limit exceeded' });
    }

    access.usedPrints += 1;
    await access.save();

    const doc = access.documentId;
    if (!doc) {
      return res.status(404).json({ message: 'Document not found' });
    }

    const bucket = process.env.AWS_S3_BUCKET;
    if (!bucket) {
      return res.status(500).json({ message: 'S3 not configured' });
    }

    // Generate a short-lived presigned URL so browser securely fetches from S3 without AccessDenied
    const command = new GetObjectCommand({
      Bucket: bucket,
      Key: doc.fileKey,
    });

    const signedUrl = await getSignedUrl(s3, command, { expiresIn: 60 }); // 60 seconds

    return res.json({
      fileUrl: signedUrl,
      remainingPrints: access.assignedQuota - access.usedPrints,
      maxPrints: access.assignedQuota,
    });
  } catch (err) {
    console.error('Secure print error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

// List documents assigned to the logged-in user, including background jobs
router.get('/assigned', authMiddleware, async (req, res) => {
  try {
    const accesses = await DocumentAccess.find({ userId: req.user._id })
      .populate('documentId')
      .sort({ createdAt: -1 });

    const accessResults = accesses.map((access) => {
      const doc = access.documentId;
      const title = doc?.title || 'Untitled Document';
      const loweredTitle = title.toLowerCase();
      const isSvg = loweredTitle.endsWith('.svg');

      return {
        id: access._id,
        documentId: doc?._id,
        documentTitle: title,
        assignedQuota: access.assignedQuota,
        usedPrints: access.usedPrints,
        remainingPrints: access.assignedQuota - access.usedPrints,
        sessionToken: access.sessionToken,
        documentType: isSvg ? 'svg' : 'pdf',
        status: 'completed',
      };
    });

    const jobs = await DocumentJobs.find({ userId: req.user._id })
      .sort({ createdAt: -1 })
      .exec();

    // Self-heal: if a job has all pages rendered but no merged document yet,
    // enqueue a merge job so it can complete.
    await Promise.all(
      jobs.map(async (job) => {
        if (
          job &&
          job.totalPages > 0 &&
          job.completedPages >= job.totalPages &&
          !job.outputDocumentId &&
          job.status !== 'completed' &&
          job.stage !== 'merging'
        ) {
          await mergePdfQueue.add('mergeJob', {
            jobId: job._id.toString(),
            email: job.email,
            assignedQuota: job.assignedQuota,
            adminUserId: job.createdBy,
          });

          job.stage = 'merging';
          job.status = 'processing';
          await job.save();
        }
      })
    );

    const activeJobs = jobs.filter((job) => job.status !== 'completed');

    const jobResults = activeJobs.map((job) => ({
      id: job._id,
      documentId: job.outputDocumentId || null,
      documentTitle: 'Generated Output',
      assignedQuota: job.assignedQuota,
      usedPrints: 0,
      remainingPrints: null,
      sessionToken: null,
      documentType: 'pdf',
      status: job.status,
      stage: job.stage,
      totalPages: job.totalPages || 0,
      completedPages: job.completedPages || 0,
    }));

    const combined = [...jobResults, ...accessResults];

    return res.json(combined);
  } catch (err) {
    console.error('List assigned docs error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

export default router;
