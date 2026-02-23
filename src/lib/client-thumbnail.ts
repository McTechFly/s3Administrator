import type { GalleryItem } from "@/types";
import {
  getCachedThumbnail,
  storeThumbnail,
} from "@/lib/thumbnail-db";

const THUMBNAIL_MAX_WIDTH = 480;
const THUMBNAIL_QUALITY = 0.8;

// Prevents duplicate concurrent generation for the same key
const inFlight = new Map<string, Promise<Blob | null>>();

function cacheKey(
  credentialId: string,
  bucket: string,
  key: string
): string {
  return `${credentialId}|${bucket}|${key}`;
}

async function generateImageThumbnail(url: string): Promise<Blob> {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Failed to fetch image: ${response.status}`);
  }
  const blob = await response.blob();
  const bitmap = await createImageBitmap(blob);

  const scale = Math.min(1, THUMBNAIL_MAX_WIDTH / bitmap.width);
  const width = Math.round(bitmap.width * scale);
  const height = Math.round(bitmap.height * scale);

  const canvas = new OffscreenCanvas(width, height);
  const ctx = canvas.getContext("2d");
  if (!ctx) {
    throw new Error("Failed to get 2D context");
  }
  ctx.drawImage(bitmap, 0, 0, width, height);
  bitmap.close();

  return canvas.convertToBlob({ type: "image/webp", quality: THUMBNAIL_QUALITY });
}

async function generateVideoThumbnail(url: string): Promise<Blob> {
  // Lazy-load FFmpeg WASM — only downloaded when first video is processed
  const [{ FFmpeg }, { fetchFile }] = await Promise.all([
    import("@ffmpeg/ffmpeg"),
    import("@ffmpeg/util"),
  ]);

  const ffmpeg = new FFmpeg();
  await ffmpeg.load();

  const videoData = await fetchFile(url);
  await ffmpeg.writeFile("input", videoData);

  // Try to grab a frame at 1 second; if the video is shorter, fall back to frame 0
  let exitCode = await ffmpeg.exec([
    "-ss", "1",
    "-i", "input",
    "-frames:v", "1",
    "-vf", `scale=${THUMBNAIL_MAX_WIDTH}:-1`,
    "-f", "image2",
    "out.webp",
  ]);

  if (exitCode !== 0) {
    exitCode = await ffmpeg.exec([
      "-i", "input",
      "-frames:v", "1",
      "-vf", `scale=${THUMBNAIL_MAX_WIDTH}:-1`,
      "-f", "image2",
      "out.webp",
    ]);
    if (exitCode !== 0) {
      throw new Error("FFmpeg failed to extract video frame");
    }
  }

  const data = await ffmpeg.readFile("out.webp");
  const binaryData =
    data instanceof Uint8Array
      ? data
      : new TextEncoder().encode(String(data));

  // Copy into a plain ArrayBuffer so BlobPart typing does not depend on
  // the source buffer implementation (ArrayBufferLike vs SharedArrayBuffer).
  const imageBuffer = new ArrayBuffer(binaryData.byteLength);
  new Uint8Array(imageBuffer).set(binaryData);

  // Clean up virtual FS
  await ffmpeg.deleteFile("input");
  await ffmpeg.deleteFile("out.webp");

  return new Blob([imageBuffer], { type: "image/webp" });
}

/**
 * Returns a cached or freshly-generated thumbnail blob for a gallery item.
 * Deduplicates concurrent calls for the same key.
 * Returns null if generation is not applicable or fails.
 */
export async function getOrGenerateThumbnail(
  item: GalleryItem,
  credentialId: string,
  bucket: string
): Promise<Blob | null> {
  if (item.isFolder || !item.mediaType || !item.previewUrl) {
    return null;
  }

  const ck = cacheKey(credentialId, bucket, item.key);

  // Check IndexedDB first
  try {
    const cached = await getCachedThumbnail(
      credentialId,
      bucket,
      item.key,
      item.lastModified,
      item.size
    );
    if (cached) return cached;
  } catch {
    // IndexedDB unavailable — proceed to generate without caching
  }

  // Deduplicate in-flight requests
  const existing = inFlight.get(ck);
  if (existing) return existing;

  const promise = (async (): Promise<Blob | null> => {
    try {
      const blob = item.isVideo
        ? await generateVideoThumbnail(item.previewUrl!)
        : await generateImageThumbnail(item.previewUrl!);

      try {
        await storeThumbnail(
          credentialId,
          bucket,
          item.key,
          item.lastModified,
          item.size,
          blob
        );
      } catch {
        // Storage failure is non-fatal
      }

      return blob;
    } catch {
      return null;
    } finally {
      inFlight.delete(ck);
    }
  })();

  inFlight.set(ck, promise);
  return promise;
}
