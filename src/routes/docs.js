import express from 'express';
import multer from 'multer';
import crypto from 'crypto';
import { PassThrough } from 'stream';
import mongoose from 'mongoose';
import Document from '../vectorModels/VectorDocument.js';
import DocumentAccess from '../vectorModels/VectorDocumentAccess.js';
import DocumentJobs from '../vectorModels/VectorDocumentJobs.js';
import VectorPrintJob from '../vectorModels/VectorPrintJob.js';
import { uploadToS3WithKey, s3, downloadFromS3 } from '../services/s3.js';
import { GetObjectCommand, PutObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { authMiddleware } from '../middleware/auth.js';
import { A4_WIDTH, A4_HEIGHT, SAFE_MARGIN } from '../vector/constants.js';
import { assertAndConsumePrintQuota } from '../services/printQuotaService.js';
import { resolveFinalPdfKeyForServe } from '../services/finalPdfExportService.js';
import { signJobPayload, getStableHmacPayload } from '../services/hmac.js';
import { traceLog } from '../services/traceLog.js';
import { svgBytesToPdfBytes } from '../vector/vectorLayoutEngine.js';

const router = express.Router();
const upload = multer();

router.post('/:documentId/generate', authMiddleware, async (req, res) => {
  try {
    const { documentId } = req.params;
    if (!documentId) {
      return res.status(400).json({ message: 'documentId is required' });
    }

    const access = await DocumentAccess.findOne({ documentId, userId: req.user._id, revoked: false })
      .populate({ path: 'documentId', model: 'VectorDocument' })
      .exec();

    if (!access) {
      return res.status(404).json({ message: 'Access not found' });
    }

    const doc = access.documentId;
    if (!doc) {
      return res.status(404).json({ message: 'Document not found' });
    }

    const mime = String(doc?.mimeType || '').toLowerCase();
    const sourceMime = String(doc?.sourceMimeType || '').toLowerCase();
    const isSvgDoc = mime === 'image/svg+xml' || sourceMime === 'image/svg+xml';
    if (!isSvgDoc) {
      return res.status(400).json({ message: 'Document is not an SVG' });
    }

    const finalAlready = typeof doc?.finalPdfKey === 'string' && doc.finalPdfKey.trim();
    const fileKeyAlready = typeof doc?.fileKey === 'string' && doc.fileKey.trim() && doc.fileKey.trim().toLowerCase().endsWith('.pdf');
    if (finalAlready || fileKeyAlready) {
      await Document.updateOne(
        { _id: doc._id },
        {
          $set: {
            svgStatus: 'NORMALIZED',
            svgNormalizeStatus: 'DONE',
            svgNormalizeError: { message: null, stack: null },
            normalizedAt: doc.normalizedAt || new Date(),
            normalizeFailed: false,
            normalizeError: { message: null, stack: null, reason: null, at: null },
          },
        }
      ).exec();

      return res.json({ status: 'DONE' });
    }

    const sourceKey =
      String(doc?.rawFileKey || '').trim() ||
      String(doc?.sourceFileKey || '').trim() ||
      String(doc?.fileKey || '').trim();
    if (!sourceKey) {
      return res.status(400).json({ message: 'SVG source key missing' });
    }

    await Document.updateOne(
      { _id: doc._id },
      {
        $set: {
          svgStatus: 'RAW',
          svgNormalizeStatus: null,
          svgNormalizeError: { message: null, stack: null },
          svgNormalizeEnqueuedAt: null,
          svgNormalizeJobId: null,
          svgNormalizeStartedAt: null,
          normalizeFailed: false,
          normalizeError: { message: null, stack: null, reason: null, at: null },
        },
      }
    ).exec();

    const bytes = await downloadFromS3(sourceKey);
    const head = Buffer.from(bytes.slice(0, 5)).toString();
    if (head.startsWith('%PDF-')) {
      return res.status(400).json({ message: 'Document is already a PDF' });
    }

    const placement = doc?.placementRules && typeof doc.placementRules === 'object' ? doc.placementRules : null;

    const pdfBytes = await svgBytesToPdfBytes(bytes, { documentId: doc._id.toString(), placementRules: placement });

    const outKey = `generated/${doc._id.toString()}.pdf`;
    const uploaded = await uploadToS3WithKey(Buffer.from(pdfBytes), 'application/pdf', outKey);

    await Document.updateOne(
      { _id: doc._id },
      {
        $set: {
          fileKey: uploaded.key,
          fileUrl: uploaded.url,
          finalPdfKey: uploaded.key,
          svgStatus: 'NORMALIZED',
          svgNormalizeStatus: 'DONE',
          svgNormalizeError: { message: null, stack: null },
          normalizedAt: new Date(),
          normalizeFailed: false,
          normalizeError: { message: null, stack: null, reason: null, at: null },
        },
      }
    ).exec();

    return res.json({ status: 'DONE' });
  } catch (err) {
    console.error('Generate SVG error', err);
    return res.status(500).json({ message: err?.message || 'Internal server error' });
  }
});

// Helper to generate opaque session tokens
const generateSessionToken = () => crypto.randomBytes(32).toString('hex');

const readUtf8Prefix = (bytes, maxBytes) => {
  const buf = Buffer.isBuffer(bytes) ? bytes : Buffer.from(bytes || []);
  const limit = Math.max(1, Math.min(512 * 1024, Number(maxBytes || 0) || 0));
  return buf.slice(0, limit).toString('utf8');
};

const extractSvgViewBox = (svgHeader) => {
  const raw = typeof svgHeader === 'string' ? svgHeader : '';
  const m = raw.match(/viewBox\s*=\s*(['"])([^'"]+)\1/i);
  if (!m) return null;
  const parts = String(m[2] || '')
    .trim()
    .split(/[ ,]+/)
    .map((v) => Number(v));
  if (parts.length !== 4 || parts.some((n) => !Number.isFinite(n))) return null;
  const [x, y, width, height] = parts;
  if (width <= 0 || height <= 0) return null;
  return { x, y, width, height };
};

const extractSvgWidthHeightPt = (svgHeader) => {
  const raw = typeof svgHeader === 'string' ? svgHeader : '';
  const open = raw.match(/<svg\b[^>]*>/i);
  const tag = open ? open[0] : '';
  if (!tag) return null;
  const pick = (name) => {
    const m = tag.match(new RegExp(`\\b${name}\\s*=\\s*(['\"])([^'\"]+)\\1`, 'i'));
    return m ? String(m[2] || '').trim() : null;
  };
  const parseLenPt = (value) => {
    const s = typeof value === 'string' ? value.trim() : '';
    if (!s) return null;
    const m = s.match(/^([+-]?(?:\d+\.?\d*|\d*\.?\d+))(pt)?$/i);
    if (!m) return null;
    const n = Number(m[1]);
    if (!Number.isFinite(n) || n <= 0) return null;
    return n;
  };
  const w = parseLenPt(pick('width'));
  const h = parseLenPt(pick('height'));
  if (!Number.isFinite(w) || !Number.isFinite(h)) return null;
  return { widthPt: w, heightPt: h };
};

const computeEditorProxyFromSvgHeader = (svgHeader) => {
  const vb = extractSvgViewBox(svgHeader);
  const wh = extractSvgWidthHeightPt(svgHeader);

  const pageWidthPt = Number.isFinite(wh?.widthPt) ? wh.widthPt : Number(A4_WIDTH);
  const pageHeightPt = Number.isFinite(wh?.heightPt) ? wh.heightPt : Number(A4_HEIGHT);

  if (!vb) {
    return {
      page: { widthPt: pageWidthPt, heightPt: pageHeightPt },
      contentBBox: { x: 0, y: 0, width: pageWidthPt, height: pageHeightPt },
      viewBox: null,
      viewBoxTransform: null,
    };
  }

  const scale = Math.min(pageWidthPt / vb.width, pageHeightPt / vb.height);
  const translateX = -vb.x * scale + (pageWidthPt - vb.width * scale) / 2;
  const translateY = -vb.y * scale + (pageHeightPt - vb.height * scale) / 2;

  return {
    page: { widthPt: pageWidthPt, heightPt: pageHeightPt },
    contentBBox: {
      x: (pageWidthPt - vb.width * scale) / 2,
      y: (pageHeightPt - vb.height * scale) / 2,
      width: vb.width * scale,
      height: vb.height * scale,
    },
    viewBox: vb,
    viewBoxTransform: { scale, translateX, translateY },
  };
};

// Upload document (PDF/SVG) for the logged-in user and create access record
router.post('/upload', authMiddleware, upload.single('file'), async (req, res) => {
  try {
    const { title, totalPrints } = req.body;
    const ticketCropMmRaw = req.body?.ticketCropMm;
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

    const loweredName = title.toLowerCase();
    const isSvg = file.mimetype === 'image/svg+xml' || loweredName.endsWith('.svg');

    const uploadMime = isSvg ? 'image/svg+xml' : 'application/pdf';

    const docId = new mongoose.Types.ObjectId();

    let key = '';
    let url = '';
    let sourceKey = null;
    let sourceMime = null;

    let editorProxy = null;

    if (isSvg) {
      sourceKey = `documents/raw/${docId.toString()}.svg`;
      sourceMime = 'image/svg+xml';
      const uploaded = await uploadToS3WithKey(file.buffer, sourceMime, sourceKey);

      key = uploaded.key;
      url = uploaded.url;

      try {
        const head = readUtf8Prefix(file.buffer, 256 * 1024);
        editorProxy = computeEditorProxyFromSvgHeader(head);
      } catch {
        editorProxy = computeEditorProxyFromSvgHeader('');
      }
    } else {
      const originalKey = `documents/raw/${docId.toString()}.pdf`;
      const uploaded = await uploadToS3WithKey(file.buffer, uploadMime, originalKey);
      key = uploaded.key;
      url = uploaded.url;
      sourceKey = uploaded.key;
      sourceMime = uploadMime;
    }

    const doc = await Document.create({
      _id: docId,
      title,
      fileKey: key,
      fileUrl: url,
      sourceFileKey: sourceKey,
      sourceMimeType: sourceMime,
      rawFileKey: sourceKey,
      finalPdfKey: isSvg ? null : key,
      svgStatus: isSvg ? 'RAW' : 'NORMALIZED',
      totalPrints: parsedTotal,
      createdBy: req.user._id,
      mimeType: uploadMime,
      ...(isSvg
        ? {
            editorProxy,
            svgNormalizeStatus: null,
            svgNormalizeError: { message: null, stack: null },
          }
        : {}),
    });

    if (isSvg) {
      const sizeMb = Math.round(((Number(file?.size || 0) / (1024 * 1024)) || 0) * 100) / 100;
      console.log('[UPLOAD] SVG received', { documentId: doc._id.toString(), sizeMb });
    }

    const sessionToken = generateSessionToken();

    const access = await DocumentAccess.create({
      userId: req.user._id,
      documentId: doc._id,
      assignedQuota: parsedTotal,
      usedPrints: 0,
      printQuota: parsedTotal,
      printsUsed: 0,
      revoked: false,
      sessionToken,
    });

    const documentType = isSvg ? 'svg' : 'pdf';

    let ticketCropMm = null;
    if (typeof ticketCropMmRaw === 'string' && ticketCropMmRaw.trim()) {
      try {
        const parsed = JSON.parse(ticketCropMmRaw);
        ticketCropMm = parsed && typeof parsed === 'object' ? parsed : null;
      } catch {
        ticketCropMm = null;
      }
    }

    if (ticketCropMm) {
      await Document.updateOne({ _id: doc._id }, { $set: { ticketCropMm } }).exec();
    }

    return res.status(201).json({
      sessionToken,
      document: {
        title: doc.title,
        documentId: doc._id,
        ...(isSvg ? { editorReady: true } : {}),
        remainingPrints: access.printQuota - access.printsUsed,
        maxPrints: access.printQuota,
        documentType,
        ticketCropMm,
        ...(isSvg ? { status: 'idle' } : {}),
      },
    });
  } catch (err) {
    console.error('Docs upload error', err);
    const msg = err instanceof Error ? err.message : '';
    const statusFromAws = Number.isFinite(Number(err?.$metadata?.httpStatusCode)) ? Number(err.$metadata.httpStatusCode) : null;
    const codeFromAws = typeof err?.name === 'string' && err.name.trim() ? err.name.trim() : null;

    if (typeof msg === 'string' && (msg.startsWith('INKSCAPE_NOT_FOUND:') || msg.startsWith('INKSCAPE_UNAVAILABLE:'))) {
      return res.status(503).json({ message: msg, statusCode: 503 });
    }

    if (typeof msg === 'string' && (msg.includes('S3_BUCKET not configured') || msg.includes('S3_REGION') || msg.includes('S3_ACCESS_KEY_ID') || msg.includes('S3_SECRET_ACCESS_KEY') || msg.includes('S3_ENDPOINT'))) {
      return res.status(500).json({ message: msg, statusCode: 500 });
    }

    if (statusFromAws && statusFromAws >= 400 && statusFromAws < 600) {
      return res.status(statusFromAws).json({
        message: msg || 'S3 operation failed',
        statusCode: statusFromAws,
        code: codeFromAws,
      });
    }

    return res.status(500).json({
      message: msg || 'Internal server error',
      statusCode: 500,
      code: codeFromAws,
    });
  }
});

router.get('/:documentId/editor-proxy', authMiddleware, async (req, res) => {
  try {
    const { documentId } = req.params;
    if (!documentId) {
      return res.status(400).json({ message: 'documentId is required' });
    }

    const access = await DocumentAccess.findOne({ documentId, userId: req.user._id, revoked: false })
      .populate({ path: 'documentId', model: 'VectorDocument' })
      .exec();

    if (!access) {
      return res.status(404).json({ message: 'Access not found' });
    }

    const doc = access.documentId;
    if (!doc) {
      return res.status(404).json({ message: 'Document not found' });
    }

    const pageW = Number(doc?.editorProxy?.page?.widthPt) || Number(A4_WIDTH);
    const pageH = Number(doc?.editorProxy?.page?.heightPt) || Number(A4_HEIGHT);

    const content = doc?.editorProxy?.contentBBox || null;
    const contentBBox = {
      x: Number(content?.x) || 0,
      y: Number(content?.y) || 0,
      width: Number(content?.width) || pageW,
      height: Number(content?.height) || pageH,
    };

    const sm = Number(SAFE_MARGIN) || 0;
    const safeMargins = { top: sm, right: sm, bottom: sm, left: sm };

    return res.json({
      page: { widthPt: pageW, heightPt: pageH },
      contentBBox,
      safeMargins,
    });
  } catch (err) {
    console.error('Editor proxy error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

router.put('/:documentId/placement-rules', authMiddleware, async (req, res) => {
  try {
    const { documentId } = req.params;
    if (!documentId) {
      return res.status(400).json({ message: 'documentId is required' });
    }

    const access = await DocumentAccess.findOne({ documentId, userId: req.user._id, revoked: false })
      .populate({ path: 'documentId', model: 'VectorDocument' })
      .exec();

    if (!access) {
      return res.status(404).json({ message: 'Access not found' });
    }

    const doc = access.documentId;
    if (!doc) {
      return res.status(404).json({ message: 'Document not found' });
    }

    const seriesPlacement = req.body?.seriesPlacement;
    const anchor = typeof seriesPlacement?.anchor === 'string' ? seriesPlacement.anchor.trim() : '';
    const offsetX = Number(seriesPlacement?.offset?.x);
    const offsetY = Number(seriesPlacement?.offset?.y);
    const rotation = Number(seriesPlacement?.rotation || 0);

    if (!anchor) {
      return res.status(400).json({ message: 'seriesPlacement.anchor is required' });
    }
    if (!Number.isFinite(offsetX) || !Number.isFinite(offsetY)) {
      return res.status(400).json({ message: 'seriesPlacement.offset.x and .y must be finite numbers' });
    }
    if (!Number.isFinite(rotation)) {
      return res.status(400).json({ message: 'seriesPlacement.rotation must be a finite number' });
    }

    await Document.updateOne(
      { _id: doc._id },
      {
        $set: {
          placementRules: {
            seriesPlacement: {
              anchor,
              offset: { x: offsetX, y: offsetY },
              rotation,
            },
          },
        },
      }
    ).exec();

    return res.json({ ok: true });
  } catch (err) {
    console.error('Placement rules save error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

router.get('/:documentId/placement-rules', authMiddleware, async (req, res) => {
  try {
    const { documentId } = req.params;
    if (!documentId) {
      return res.status(400).json({ message: 'documentId is required' });
    }

    const access = await DocumentAccess.findOne({ documentId, userId: req.user._id, revoked: false })
      .populate({ path: 'documentId', model: 'VectorDocument' })
      .exec();

    if (!access) {
      return res.status(404).json({ message: 'Access not found' });
    }

    const doc = access.documentId;
    if (!doc) {
      return res.status(404).json({ message: 'Document not found' });
    }

    const rules = doc?.placementRules && typeof doc.placementRules === 'object' ? doc.placementRules : null;
    return res.json({ placementRules: rules });
  } catch (err) {
    console.error('Placement rules fetch error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

// Raw SVG fetch (source of truth for SVG rendering). sessionStorage is cache only.
router.get('/:documentId/raw-svg', async (req, res) => {
  const origin = req.headers.origin;
  if (origin) {
    res.setHeader('Access-Control-Allow-Origin', origin);
    res.setHeader('Vary', 'Origin');
  }
  res.setHeader('Access-Control-Allow-Credentials', 'true');

  try {
    const { documentId } = req.params;
    if (!documentId) {
      return res.status(400).json({ message: 'documentId is required' });
    }

    const authHeader = String(req.headers.authorization || '');
    const hasBearer = authHeader.startsWith('Bearer ');

    let access = null;
    if (hasBearer) {
      await new Promise((resolve) => authMiddleware(req, res, resolve));
      if (res.headersSent) return;

      access = await DocumentAccess.findOne({ documentId, userId: req.user._id, revoked: false })
        .populate({ path: 'documentId', model: 'VectorDocument' })
        .exec();
    } else {
      const sessionToken = String(req.headers['x-session-token'] || '').trim();
      if (!sessionToken) {
        return res.status(401).json({ logout: true, message: 'Unauthorized' });
      }

      access = await DocumentAccess.findOne({ documentId, sessionToken, revoked: false })
        .populate({ path: 'documentId', model: 'VectorDocument' })
        .exec();
    }

    if (!access) {
      return res.status(404).json({ message: 'Access not found' });
    }

    const doc = access.documentId;
    if (!doc) {
      return res.status(404).json({ message: 'Document not found' });
    }

    const primaryKey = doc.sourceFileKey || doc.fileKey;
    if (!primaryKey) {
      return res.status(404).json({ message: 'Source file not found' });
    }

    const candidateKeys = [primaryKey];
    // Legacy fallback: older SVG uploads stored the render key as documents/original/<uuid>.pdf
    // but the immutable SVG was uploaded at documents/source/<uuid>.svg (not persisted in DB).
    if (!doc.sourceFileKey && typeof doc.fileKey === 'string') {
      const m = doc.fileKey.match(/^documents\/original\/([^/]+)\.pdf$/i);
      if (m && m[1]) {
        candidateKeys.unshift(`documents/source/${m[1]}.svg`);
      }
    }

    let bytes = null;
    let keyUsed = '';
    for (const k of candidateKeys) {
      try {
        const b = await downloadFromS3(k);
        const prefix = Buffer.from(b.slice(0, 2048)).toString('utf8').toLowerCase();
        const head = Buffer.from(b.slice(0, 5)).toString();
        if (head.startsWith('%PDF-')) continue;
        if (!prefix.includes('<svg')) continue;
        bytes = b;
        keyUsed = k;
        break;
      } catch {
        // try next key
      }
    }

    if (!bytes) {
      return res.status(400).json({ message: 'Document is not an SVG', keyChecked: primaryKey });
    }

    res.setHeader('Content-Type', 'image/svg+xml; charset=utf-8');
    res.setHeader('Cache-Control', 'no-store');
    res.setHeader('X-Source-Key', keyUsed);
    return res.send(bytes);
  } catch (err) {
    console.error('Raw SVG fetch error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

router.get('/:documentId/status', authMiddleware, async (req, res) => {
  try {
    const { documentId } = req.params;
    if (!documentId) {
      return res.status(400).json({ message: 'documentId is required' });
    }

    const access = await DocumentAccess.findOne({ documentId, userId: req.user._id, revoked: false })
      .populate({ path: 'documentId', model: 'VectorDocument' })
      .exec();

    if (!access) {
      return res.status(404).json({ message: 'Access not found' });
    }

    const doc = access.documentId;
    if (!doc) {
      return res.status(404).json({ message: 'Document not found' });
    }

    const simplified = String(doc?.svgStatus || '').trim().toUpperCase();
    const status = simplified === 'NORMALIZED'
      ? 'DONE'
      : simplified === 'ERROR'
        ? 'FAILED'
        : 'IDLE';
    const errMsg = String(doc?.normalizeError?.message || doc?.svgNormalizeError?.message || '').trim();

    return res.json({
      status,
      normalizedAt: doc?.normalizedAt || null,
      error: status === 'FAILED' ? { message: errMsg || 'SVG normalization failed' } : null,
    });
  } catch (err) {
    console.error('Status check error', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});

// Secure render: stream PDF/SVG bytes based on session token
router.post('/secure-render', authMiddleware, async (req, res) => {
  try {
    const { sessionToken, requestId } = req.body;

    if (!sessionToken) {
      return res.status(400).json({ message: 'sessionToken is required' });
    }

    const access = await DocumentAccess.findOne({ sessionToken }).populate({
      path: 'documentId',
      model: 'VectorDocument',
    });
    if (!access) {
      return res.status(404).json({ message: 'Access not found' });
    }

    if (access.userId.toString() !== req.user._id.toString()) {
      return res.status(409).json({ message: 'Not authorized for this document' });
    }

    if (access.revoked) {
      return res.status(403).json({ message: 'Access revoked' });
    }

    const doc = access.documentId;
    if (!doc) {
      return res.status(404).json({ message: 'Document not found' });
    }

    const mime = String(doc?.mimeType || '').toLowerCase();
    const sourceMime = String(doc?.sourceMimeType || '').toLowerCase();
    const isSvgDoc = mime === 'image/svg+xml' || sourceMime === 'image/svg+xml';

    const docIdStr = doc._id?.toString?.() || String(doc._id || '');
    let jobDoc = await VectorPrintJob.findOne({
      userId: req.user._id,
      'metadata.documentId': docIdStr,
      'metadata.assignmentId': { $exists: false },
    })
      .sort({ createdAt: -1 })
      .exec();

    if (!jobDoc) {
      const keyStr = typeof doc?.fileKey === 'string' ? doc.fileKey.trim() : '';
      const isPdf = keyStr.toLowerCase().endsWith('.pdf');
      const svgStatus = String(doc?.svgStatus || '').trim().toUpperCase();

      if (isSvgDoc && !(svgStatus === 'NORMALIZED' || isPdf)) {
        return res.status(409).json({ message: 'Document is still preparing. Please wait…' });
      }

      const incomingRequestId =
        (typeof req.headers['x-request-id'] === 'string' && req.headers['x-request-id'].trim())
          ? String(req.headers['x-request-id']).trim()
          : (typeof requestId === 'string' && requestId.trim())
            ? requestId.trim()
            : crypto.randomUUID();

      await assertAndConsumePrintQuota(doc._id.toString(), req.user._id.toString(), incomingRequestId);

      const bucket = process.env.S3_BUCKET;
      if (!bucket) {
        return res.status(500).json({ message: 'S3 not configured' });
      }

      const serveKey = await resolveFinalPdfKeyForServe(docIdStr);

      const command = new GetObjectCommand({
        Bucket: bucket,
        Key: serveKey,
      });

      const s3Response = await s3.send(command);
      if (!s3Response?.Body) {
        return res.status(404).json({ message: 'File not found' });
      }

      res.setHeader('Content-Type', 'application/pdf');
      res.setHeader('Cache-Control', 'no-store');
      if (s3Response.ContentLength !== undefined && s3Response.ContentLength !== null) {
        res.setHeader('Content-Length', String(s3Response.ContentLength));
      }

      const body = s3Response.Body;
      body.on('error', () => {
        try {
          res.end();
        } catch {
          // ignore
        }
      });

      const tee = new PassThrough();
      let headerChecked = false;
      let buffered = Buffer.alloc(0);

      tee.on('data', (chunk) => {
        if (headerChecked) return;
        buffered = Buffer.concat([buffered, Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)]);
        if (buffered.length >= 5) {
          headerChecked = true;
          const header = Buffer.from(buffered.slice(0, 5)).toString();
          if (!header.startsWith('%PDF-')) {
            try {
              res.destroy(new Error('SECURITY VIOLATION: Output is not a valid PDF. Vector pipeline broken.'));
            } catch {
              // ignore
            }
          }
        }
      });

      body.pipe(tee);
      return tee.pipe(res);
    }

    traceLog({
      traceId: jobDoc?.traceId,
      jobId: jobDoc._id.toString(),
      event: 'SECURE_RENDER_REQUEST',
      payload: { documentId: docIdStr, status: String(jobDoc.status || '') },
    });

    const inProgress = ['CREATED', 'BATCH_RUNNING', 'MERGE_RUNNING'].includes(String(jobDoc.status || ''));
    const updatedAt = jobDoc?.updatedAt instanceof Date ? jobDoc.updatedAt : null;
    const staleMs = 6 * 60 * 60 * 1000;
    if (inProgress && updatedAt && Date.now() - updatedAt.getTime() > staleMs) {
      const cutoff = new Date(Date.now() - staleMs);
      const fromStatus = String(jobDoc.status);
      const updated = await VectorPrintJob.findOneAndUpdate(
        {
          _id: jobDoc._id,
          status: fromStatus,
          updatedAt: { $lte: cutoff },
        },
        {
          $set: {
            status: 'FAILED',
            errorAt: new Date(),
            errorCode: 'TIMEOUT',
            error: { message: 'Job timed out', stack: null },
          },
          $push: {
            audit: { event: 'JOB_TIMEOUT', details: { fromStatus } },
            lifecycleHistory: { from: fromStatus, to: 'FAILED', at: new Date(), source: 'api' },
          },
        },
        { new: true }
      )
        .exec()
        .catch(() => null);

      if (updated) {
        jobDoc = updated;
      }
    }

    if (jobDoc.status !== 'READY') {
      const errCode = jobDoc.status === 'FAILED' ? String(jobDoc.errorCode || 'UNKNOWN') : null;
      const canRetry =
        jobDoc.status === 'FAILED'
          ? !['HMAC_FAILED', 'SVG_TOO_COMPLEX', 'INVALID_INPUT'].includes(errCode)
          : false;

      traceLog({
        traceId: jobDoc?.traceId,
        jobId: jobDoc._id.toString(),
        event: 'SECURE_RENDER_STATUS',
        payload: { status: jobDoc.status, canRetry, retryAfterMs: jobDoc.status === 'FAILED' ? 0 : 3000, errorCode: errCode },
      });

      return res.status(200).json({
        status: jobDoc.status,
        canRetry,
        retryAfterMs: jobDoc.status === 'FAILED' ? 0 : 3000,
        errorCode: errCode,
      });
    }

    const incomingRequestId =
      (typeof req.headers['x-request-id'] === 'string' && req.headers['x-request-id'].trim())
        ? String(req.headers['x-request-id']).trim()
        : (typeof requestId === 'string' && requestId.trim())
          ? requestId.trim()
          : crypto.randomUUID();

    await assertAndConsumePrintQuota(doc._id.toString(), req.user._id.toString(), incomingRequestId);

    const bucket = process.env.S3_BUCKET;
    if (!bucket) {
      return res.status(500).json({ message: 'S3 not configured' });
    }

    const serveKey = await resolveFinalPdfKeyForServe(docIdStr);

    traceLog({
      traceId: jobDoc?.traceId,
      jobId: jobDoc._id.toString(),
      event: 'SECURE_RENDER_READY',
      payload: { documentId: doc._id?.toString?.() || String(doc._id || ''), key: serveKey },
    });

    const command = new GetObjectCommand({
      Bucket: bucket,
      Key: serveKey,
    });

    const s3Response = await s3.send(command);
    if (!s3Response?.Body) {
      return res.status(404).json({ message: 'File not found' });
    }

    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Cache-Control', 'no-store');
    if (s3Response.ContentLength !== undefined && s3Response.ContentLength !== null) {
      res.setHeader('Content-Length', String(s3Response.ContentLength));
    }

    traceLog({
      traceId: jobDoc?.traceId,
      jobId: jobDoc._id.toString(),
      event: 'SECURE_RENDER_200',
      payload: { documentId: doc._id?.toString?.() || String(doc._id || ''), key: serveKey },
    });

    const body = s3Response.Body;
    body.on('error', () => {
      try {
        res.end();
      } catch {
        // ignore
      }
    });

    const tee = new PassThrough();
    let headerChecked = false;
    let buffered = Buffer.alloc(0);

    tee.on('data', (chunk) => {
      if (headerChecked) return;
      buffered = Buffer.concat([buffered, Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)]);
      if (buffered.length >= 5) {
        headerChecked = true;
        const header = Buffer.from(buffered.slice(0, 5)).toString();
        if (!header.startsWith('%PDF-')) {
          try {
            res.destroy(new Error('SECURITY VIOLATION: Output is not a valid PDF. Vector pipeline broken.'));
          } catch {
            // ignore
          }
        }
      }
    });

    body.pipe(tee);
    return tee.pipe(res);
  } catch (err) {
    console.error('Secure render error', err);
    if (err && (err.code === 'LIMIT' || /print limit exceeded/i.test(String(err.message || '')))) {
      return res.status(403).json({ message: 'Print limit exceeded' });
    }
    if (err && (err.code === 'REVOKED' || /access revoked/i.test(String(err.message || '')))) {
      return res.status(403).json({ message: 'Access revoked' });
    }
    return res.status(500).json({ message: 'Internal server error' });
  }
});

// Secure print: decrement quota and return presigned S3 URL for printing
router.post('/secure-print', authMiddleware, async (req, res) => {
  try {
    const { sessionToken, requestId } = req.body;

    if (!sessionToken) {
      return res.status(400).json({ message: 'sessionToken is required' });
    }

    const access = await DocumentAccess.findOne({ sessionToken }).populate({
      path: 'documentId',
      model: 'VectorDocument',
    });
    if (!access) {
      return res.status(404).json({ message: 'Access not found' });
    }

    if (access.userId.toString() !== req.user._id.toString()) {
      return res.status(403).json({ message: 'Not authorized for this document' });
    }

    if (access.revoked) {
      return res.status(403).json({ message: 'Access revoked' });
    }

    const docId = access.documentId?._id;
    if (!docId) {
      return res.status(404).json({ message: 'Document not found' });
    }

    const incomingRequestId =
      (typeof req.headers['x-request-id'] === 'string' && req.headers['x-request-id'].trim())
        ? String(req.headers['x-request-id']).trim()
        : (typeof requestId === 'string' && requestId.trim())
          ? requestId.trim()
          : crypto.randomUUID();

    await assertAndConsumePrintQuota(docId.toString(), req.user._id.toString(), incomingRequestId);

    const doc = access.documentId;
    if (!doc) {
      return res.status(404).json({ message: 'Document not found' });
    }

    const bucket = process.env.S3_BUCKET;
    if (!bucket) {
      return res.status(500).json({ message: 'S3 not configured' });
    }

    // Generate a short-lived presigned URL so browser securely fetches from S3 without AccessDenied
    const serveKey = await resolveFinalPdfKeyForServe(docId.toString());
    const command = new GetObjectCommand({
      Bucket: bucket,
      Key: serveKey,
    });

    const signedUrl = await getSignedUrl(s3, command, { expiresIn: 60 }); // 60 seconds

    const refreshed = await DocumentAccess.findById(access._id)
      .select('printQuota printsUsed assignedQuota usedPrints')
      .exec();

    const maxPrints = Number.isFinite(refreshed?.printQuota) ? refreshed.printQuota : refreshed?.assignedQuota;
    const usedPrints = Number.isFinite(refreshed?.printsUsed) ? refreshed.printsUsed : refreshed?.usedPrints;
    const remainingPrints = Number.isFinite(maxPrints) && Number.isFinite(usedPrints) ? maxPrints - usedPrints : null;

    return res.json({
      fileUrl: signedUrl,
      remainingPrints,
      maxPrints,
    });
  } catch (err) {
    console.error('Secure print error', err);
    if (err && (err.code === 'LIMIT' || /print limit exceeded/i.test(String(err.message || '')))) {
      return res.status(403).json({ message: 'Print limit exceeded' });
    }
    if (err && /access revoked/i.test(String(err.message || ''))) {
      return res.status(403).json({ message: 'Access revoked' });
    }
    return res.status(500).json({ message: err?.message || 'Internal server error' });
  }
});

// List documents assigned to the logged-in user, including background jobs
router.get('/assigned', authMiddleware, async (req, res) => {
  try {
    const accesses = await DocumentAccess.find({ userId: req.user._id })
      .populate({ path: 'documentId', model: 'VectorDocument' })
      .sort({ createdAt: -1 });

    const accessResults = accesses.map((access) => {
      const doc = access.documentId;
      const title = doc?.title || 'Untitled Document';
      const mime = doc?.mimeType || 'application/pdf';
      const isSvg = mime === 'image/svg+xml';

      const quota = Number.isFinite(access.printQuota) ? access.printQuota : access.assignedQuota;
      const used = Number.isFinite(access.printsUsed) ? access.printsUsed : access.usedPrints;

      return {
        id: access._id,
        documentId: doc?._id,
        documentTitle: title,
        assignedQuota: quota,
        usedPrints: used,
        remainingPrints: quota - used,
        sessionToken: access.sessionToken,
        documentType: isSvg ? 'svg' : 'pdf',
        status: 'completed',
      };
    });

    const jobs = await DocumentJobs.find({ userId: req.user._id })
      .sort({ createdAt: -1 })
      .exec();

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
