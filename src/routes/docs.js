import express from 'express';
import multer from 'multer';
import crypto from 'crypto';

import Document from '../models/Document.js';
import DocumentAccess from '../models/DocumentAccess.js';

import { uploadToS3, s3 } from '../services/s3.js';
import { GetObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';

import { authMiddleware } from '../middleware/auth.js';

const router = express.Router();
const upload = multer();

// Helper to generate opaque session tokens
const generateSessionToken = () =>
  crypto.randomBytes(32).toString('hex');

/**
 * UPLOAD DOCUMENT (PDF / SVG)
 */
router.post(
  '/upload',
  authMiddleware,
  upload.single('file'),
  async (req, res) => {
    try {
      const { title, totalPrints } = req.body;
      const file = req.file;

      if (!file) {
        return res.status(400).json({ message: 'File is required' });
      }

      const parsedTotal = parseInt(totalPrints, 10);
      if (!parsedTotal || parsedTotal <= 0) {
        return res
          .status(400)
          .json({ message: 'totalPrints must be a positive number' });
      }

      const { key, url } = await uploadToS3(
        file.buffer,
        file.mimetype,
        'securepdf/'
      );

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

      const loweredName = (title || '').toLowerCase();
      const isSvg =
        file.mimetype === 'image/svg+xml' ||
        loweredName.endsWith('.svg');

      return res.status(201).json({
        sessionToken,
        documentTitle: doc.title,
        documentId: doc._id,
        remainingPrints:
          access.assignedQuota - access.usedPrints,
        maxPrints: access.assignedQuota,
        documentType: isSvg ? 'svg' : 'pdf',
      });
    } catch (err) {
      console.error('Docs upload error', err);
      return res
        .status(500)
        .json({ message: 'Internal server error' });
    }
  }
);

/**
 * SECURE RENDER (stream PDF / SVG)
 */
router.post(
  '/secure-render',
  authMiddleware,
  async (req, res) => {
    try {
      const { sessionToken } = req.body;

      if (!sessionToken) {
        return res
          .status(400)
          .json({ message: 'sessionToken is required' });
      }

      const access = await DocumentAccess.findOne({
        sessionToken,
      }).populate('documentId');

      if (!access) {
        return res
          .status(404)
          .json({ message: 'Access not found' });
      }

      if (
        access.userId.toString() !==
        req.user._id.toString()
      ) {
        return res
          .status(403)
          .json({ message: 'Not authorized' });
      }

      const doc = access.documentId;
      if (!doc) {
        return res
          .status(404)
          .json({ message: 'Document not found' });
      }

      const bucket = process.env.AWS_S3_BUCKET;
      if (!bucket) {
        return res
          .status(500)
          .json({ message: 'S3 not configured' });
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

      res.setHeader(
        'Content-Type',
        isSvg ? 'image/svg+xml' : 'application/pdf'
      );

      return res.send(buffer);
    } catch (err) {
      console.error('Secure render error', err);
      return res
        .status(500)
        .json({ message: 'Internal server error' });
    }
  }
);

/**
 * SECURE PRINT (presigned S3 URL)
 */
router.post(
  '/secure-print',
  authMiddleware,
  async (req, res) => {
    try {
      const { sessionToken } = req.body;

      if (!sessionToken) {
        return res
          .status(400)
          .json({ message: 'sessionToken is required' });
      }

      const access = await DocumentAccess.findOne({
        sessionToken,
      }).populate('documentId');

      if (!access) {
        return res
          .status(404)
          .json({ message: 'Access not found' });
      }

      const remaining =
        access.assignedQuota - access.usedPrints;

      if (remaining <= 0) {
        return res
          .status(400)
          .json({ message: 'Print limit exceeded' });
      }

      access.usedPrints += 1;
      await access.save();

      const doc = access.documentId;
      if (!doc) {
        return res
          .status(404)
          .json({ message: 'Document not found' });
      }

      const bucket = process.env.AWS_S3_BUCKET;
      if (!bucket) {
        return res
          .status(500)
          .json({ message: 'S3 not configured' });
      }

      const command = new GetObjectCommand({
        Bucket: bucket,
        Key: doc.fileKey,
      });

      const signedUrl = await getSignedUrl(
        s3,
        command,
        { expiresIn: 60 }
      );

      return res.json({
        fileUrl: signedUrl,
        remainingPrints:
          access.assignedQuota - access.usedPrints,
        maxPrints: access.assignedQuota,
      });
    } catch (err) {
      console.error('Secure print error', err);
      return res
        .status(500)
        .json({ message: 'Internal server error' });
    }
  }
);

/**
 * LIST ASSIGNED DOCUMENTS
 */
router.get(
  '/assigned',
  authMiddleware,
  async (req, res) => {
    try {
      const accesses = await DocumentAccess.find({
        userId: req.user._id,
      })
        .populate('documentId')
        .sort({ createdAt: -1 });

      const results = accesses.map((access) => {
        const doc = access.documentId;
        const title = doc?.title || 'Untitled Document';
        const isSvg = title.toLowerCase().endsWith('.svg');

        return {
          id: access._id,
          documentId: doc?._id || null,
          documentTitle: title,
          assignedQuota: access.assignedQuota,
          usedPrints: access.usedPrints,
          remainingPrints:
            access.assignedQuota - access.usedPrints,
          sessionToken: access.sessionToken,
          documentType: isSvg ? 'svg' : 'pdf',
          status: 'completed',
        };
      });

      return res.json(results);
    } catch (err) {
      console.error('List assigned docs error', err);
      return res
        .status(500)
        .json({ message: 'Internal server error' });
    }
  }
);

export default router;
