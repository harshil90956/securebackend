import { S3Client, PutObjectCommand, GetObjectCommand, DeleteObjectCommand } from '@aws-sdk/client-s3';
import crypto from 'crypto';
const endpointRaw = process.env.S3_ENDPOINT || process.env.AWS_S3_ENDPOINT;
const regionRaw = process.env.S3_REGION || process.env.AWS_REGION;
const bucketRaw = process.env.S3_BUCKET || process.env.AWS_S3_BUCKET;
const accessKeyIdRaw = process.env.S3_ACCESS_KEY_ID || process.env.AWS_ACCESS_KEY_ID;
const secretAccessKeyRaw = process.env.S3_SECRET_ACCESS_KEY || process.env.AWS_SECRET_ACCESS_KEY;

const endpoint = typeof endpointRaw === 'string' && endpointRaw.trim() ? endpointRaw.trim() : undefined;
const region = typeof regionRaw === 'string' && regionRaw.trim() ? regionRaw.trim() : undefined;
const bucket = typeof bucketRaw === 'string' && bucketRaw.trim() ? bucketRaw.trim() : undefined;
const accessKeyId = typeof accessKeyIdRaw === 'string' && accessKeyIdRaw.trim() ? accessKeyIdRaw.trim() : undefined;
const secretAccessKey = typeof secretAccessKeyRaw === 'string' && secretAccessKeyRaw.trim() ? secretAccessKeyRaw.trim() : undefined;

console.log('[S3_CONFIG]', {
  hasEndpoint: Boolean(endpoint),
  hasRegion: Boolean(region),
  hasBucket: Boolean(bucket),
  hasAccessKeyId: Boolean(accessKeyId),
  hasSecretAccessKey: Boolean(secretAccessKey),
  bucket: bucket ? String(bucket) : null,
  region: region ? String(region) : null,
});

if (!region || !bucket || !accessKeyId || !secretAccessKey) {
  console.warn(
    'S3 config missing: set S3_REGION/S3_BUCKET/S3_ACCESS_KEY_ID/S3_SECRET_ACCESS_KEY (or AWS_REGION/AWS_S3_BUCKET/AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY) - S3 uploads will fail until configured'
  );
}

export const s3 = new S3Client({
  endpoint,
  region,
  credentials: {
    accessKeyId: accessKeyId || '',
    secretAccessKey: secretAccessKey || '',
  },
  forcePathStyle: false,
});

export const uploadToS3 = async (fileBuffer, contentType, prefix = 'securepdf/') => {
  if (!bucket) {
    throw new Error('S3_BUCKET/AWS_S3_BUCKET not configured');
  }

  const key = `${prefix}${crypto.randomUUID()}`;

  const command = new PutObjectCommand({
    Bucket: bucket,
    Key: key,
    Body: fileBuffer,
    ContentType: contentType,
  });

  await s3.send(command);

  return { key, url: `s3://${bucket}/${key}` };
};

export const uploadToS3WithKey = async (fileBuffer, contentType, key) => {
  if (!bucket) {
    throw new Error('S3_BUCKET/AWS_S3_BUCKET not configured');
  }

  const normalizedKey = typeof key === 'string' ? key : '';
  if (!normalizedKey) {
    throw new Error('S3 key is required');
  }

  const command = new PutObjectCommand({
    Bucket: bucket,
    Key: normalizedKey,
    Body: fileBuffer,
    ContentType: contentType,
  });

  await s3.send(command);

  return { key: normalizedKey, url: `s3://${bucket}/${normalizedKey}` };
};

export const uploadFileToS3WithKey = async (filePath, contentType, key) => {
  throw new Error('Local file uploads are disabled. Provide a Buffer and use uploadToS3WithKey instead.');
};

export const getObjectStreamFromS3 = async (key) => {
  if (!bucket) {
    throw new Error('S3_BUCKET/AWS_S3_BUCKET not configured');
  }
  const command = new GetObjectCommand({ Bucket: bucket, Key: key });
  const response = await s3.send(command);
  return response;
};

export const downloadFromS3 = async (key) => {
  if (!bucket) {
    throw new Error('S3_BUCKET/AWS_S3_BUCKET not configured');
  }

  const command = new GetObjectCommand({ Bucket: bucket, Key: key });
  const response = await s3.send(command);

  const chunks = [];
  for await (const chunk of response.Body) {
    chunks.push(chunk);
  }

  return Buffer.concat(chunks);
};

export const deleteFromS3 = async (key) => {
  if (!bucket) {
    throw new Error('S3_BUCKET/AWS_S3_BUCKET not configured');
  }

  const command = new DeleteObjectCommand({ Bucket: bucket, Key: key });
  await s3.send(command);
};
