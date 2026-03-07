import crypto from 'crypto';
import fs from 'fs';
import path from 'path';

import sharp from 'sharp';

import { logger } from './logger.js';

const MAX_IMAGE_DIMENSION = 1024;
const IMAGE_REF_PATTERN = /\[Image: (attachments\/[^\]]+)\]/g;

export interface ProcessedMedia {
  content: string;
  relativePath: string;
  mediaType: string;
  isImage: boolean;
}

export interface ImageAttachment {
  relativePath: string;
  mediaType: string;
}

// --- Detection helpers ---

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type NormalizedMessage = any;

export function hasImageMessage(normalized: NormalizedMessage): boolean {
  return !!normalized?.imageMessage;
}

export function hasDocumentMessage(normalized: NormalizedMessage): boolean {
  return !!normalized?.documentMessage;
}

export function hasAudioMessage(normalized: NormalizedMessage): boolean {
  return !!normalized?.audioMessage;
}

export function hasVideoMessage(normalized: NormalizedMessage): boolean {
  return !!normalized?.videoMessage;
}

export function hasStickerMessage(normalized: NormalizedMessage): boolean {
  return !!normalized?.stickerMessage;
}

// --- Image processing ---

export async function processImage(
  buffer: Buffer,
  groupDir: string,
  caption: string,
): Promise<ProcessedMedia | null> {
  try {
    const attachDir = path.join(groupDir, 'attachments');
    fs.mkdirSync(attachDir, { recursive: true });

    const suffix = crypto.randomBytes(4).toString('hex');
    const filename = `img-${Date.now()}-${suffix}.jpg`;
    const filePath = path.join(attachDir, filename);

    await sharp(buffer)
      .resize(MAX_IMAGE_DIMENSION, MAX_IMAGE_DIMENSION, {
        fit: 'inside',
        withoutEnlargement: true,
      })
      .jpeg({ quality: 85 })
      .toFile(filePath);

    const relativePath = `attachments/${filename}`;
    const ref = `[Image: ${relativePath}]`;
    const content = caption ? `${ref} ${caption}` : ref;

    return {
      content,
      relativePath,
      mediaType: 'image/jpeg',
      isImage: true,
    };
  } catch (err) {
    logger.error({ err }, 'Failed to process image');
    return null;
  }
}

// --- Generic file save ---

export function saveAttachment(
  buffer: Buffer,
  groupDir: string,
  filename: string,
  mimeType: string,
  caption: string,
  fileType: string,
  usageHint?: string,
): ProcessedMedia | null {
  try {
    const attachDir = path.join(groupDir, 'attachments');
    fs.mkdirSync(attachDir, { recursive: true });

    const filePath = path.join(attachDir, filename);
    fs.writeFileSync(filePath, buffer);

    const sizeKB = Math.round(buffer.length / 1024);
    const relativePath = `attachments/${filename}`;
    const ref = `[${fileType}: ${relativePath} (${sizeKB}KB)]`;
    let content = caption ? `${caption}\n\n${ref}` : ref;
    if (usageHint) content += `\n${usageHint}`;

    return {
      content,
      relativePath,
      mediaType: mimeType,
      isImage: false,
    };
  } catch (err) {
    logger.error({ err, filename }, 'Failed to save attachment');
    return null;
  }
}

// --- Reference parsing ---

export function parseImageReferences(
  messages: Array<{ content: string }>,
): ImageAttachment[] {
  const attachments: ImageAttachment[] = [];
  for (const msg of messages) {
    let match: RegExpExecArray | null;
    IMAGE_REF_PATTERN.lastIndex = 0;
    while ((match = IMAGE_REF_PATTERN.exec(msg.content)) !== null) {
      attachments.push({
        relativePath: match[1],
        mediaType: 'image/jpeg',
      });
    }
  }
  return attachments;
}
