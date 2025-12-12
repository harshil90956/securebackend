import express from 'express';
import { generateOutputPdfBuffer } from '../pdf/generateOutputPdf.js';
import { uploadToS3 } from '../services/s3.js';
import Document from '../models/Document.js';
import { authMiddleware } from '../middleware/auth.js';

const router = express.Router();

// POST /api/generate-output-pdf
router.post('/generate-output-pdf', authMiddleware, async (req, res) => {
  try {
    const { pages } = req.body || {};

    if (!Array.isArray(pages) || pages.length === 0) {
      return res.status(400).json({ message: 'pages array is required' });
    }

    // 1) Build PDF buffer via Puppeteer
    //    Sometimes on Windows Puppeteer can randomly close the Chrome target
    //    while evaluating, causing a TargetCloseError. We retry once in that case.
    let pdfBuffer;
    let hasRetried = false;
    while (true) {
      try {
        pdfBuffer = await generateOutputPdfBuffer(pages);
        break;
      } catch (err) {
        const isTargetClosed =
          err &&
          (err.name === 'TargetCloseError' ||
            err.cause?.name === 'ProtocolError' ||
            (typeof err.message === 'string' && err.message.includes('Target closed')));

        if (isTargetClosed && !hasRetried) {
          console.warn('generate-output-pdf TargetCloseError, retrying once...');
          hasRetried = true;
          continue;
        }

        throw err;
      }
    }

    // 2) Upload to S3
    const { key, url } = await uploadToS3(pdfBuffer, 'application/pdf', 'generated/output/');

    // 3) Create Document record
    const doc = await Document.create({
      title: 'Generated Output',
      fileKey: key,
      fileUrl: url,
      totalPrints: 0,
      createdBy: req.user._id,
      mimeType: 'application/pdf',
      documentType: 'generated-output',
    });

    return res.status(201).json({
      success: true,
      fileKey: key,
      fileUrl: url,
      documentId: doc._id,
    });
  } catch (err) {
    console.error('generate-output-pdf error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

router.post('/series/generate', async (req, res) => {
  try {
    const { templateBase64, templateType, startNumber, endNumber } = req.body || {};

    if (!templateBase64 || !templateType) {
      return res.status(400).json({ message: 'templateBase64 and templateType are required' });
    }

    const start = Number(startNumber);
    const end = Number(endNumber);

    if (!Number.isFinite(start) || !Number.isFinite(end)) {
      return res.status(400).json({ message: 'startNumber and endNumber must be numbers' });
    }

    if (start >= end) {
      return res
        .status(400)
        .json({ message: 'startNumber must be less than endNumber' });
    }

    if (end - start > 1000) {
      return res
        .status(400)
        .json({ message: 'Maximum 1000 numbers per batch' });
    }

    const mimeType =
      templateType === 'application/pdf' || templateType === 'pdf'
        ? 'application/pdf'
        : 'image/svg+xml';

    const dataUrl = `data:${mimeType};base64,${templateBase64}`;

    const pages = [];
    for (let n = start; n <= end; n++) {
      pages.push({
        items: [
          {
            type: 'image',
            src: dataUrl,
            x: 0,
            y: 0,
            width: 794,
            height: 1123,
          },
          {
            type: 'text',
            text: String(n),
            x: 80,
            y: 80,
            fontSize: 48,
          },
        ],
      });
    }

    const pdfBuffer = await generateOutputPdfBuffer(pages);

    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', 'attachment; filename="series.pdf"');
    return res.send(pdfBuffer);
  } catch (err) {
    console.error('series/generate error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

export default router;
