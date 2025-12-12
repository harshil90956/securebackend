import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import crypto from 'crypto';
import dotenv from 'dotenv';

// Ensure env variables are loaded even when this module is imported before index.js
dotenv.config();

const region = process.env.AWS_REGION;
const bucket = process.env.AWS_S3_BUCKET;

if (!region || !bucket) {
  console.warn('AWS_REGION or AWS_S3_BUCKET not set - S3 uploads will fail until configured');
}

export const s3 = new S3Client({ region });

export const uploadToS3 = async (fileBuffer, contentType, prefix = 'securepdf/') => {
  if (!bucket) {
    throw new Error('AWS_S3_BUCKET not configured');
  }

  const key = `${prefix}${crypto.randomUUID()}`;

  const command = new PutObjectCommand({
    Bucket: bucket,
    Key: key,
    Body: fileBuffer,
    ContentType: contentType,
  });

  await s3.send(command);

  const urlBase = process.env.AWS_S3_PUBLIC_BASE_URL;
  const fileUrl = urlBase ? `${urlBase}/${key}` : `https://${bucket}.s3.${region}.amazonaws.com/${key}`;

  return { key, url: fileUrl };
};
