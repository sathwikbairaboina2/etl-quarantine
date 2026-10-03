import { createHash } from 'node:crypto';

export async function sha256Stream(stream: NodeJS.ReadableStream): Promise<string> {
  const h = createHash('sha256');
  for await (const chunk of stream) h.update(chunk as Uint8Array);
  return h.digest('hex');
}

export function sha256Bytes(bytes: Uint8Array | string): string {
  return createHash('sha256').update(bytes).digest('hex');
}
