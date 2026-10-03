import { Readable } from 'node:stream';
import {
  CopyObjectCommand,
  DeleteObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
  ListObjectsV2Command,
  PutObjectCommand,
} from '@aws-sdk/client-s3';
import { NotFoundError, type BucketName, type ObjectStore } from '../ports.js';

/** The slice of S3Client this adapter needs; lets unit tests pass a hand-written fake. */
export interface S3Like {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  send(command: any): Promise<any>;
}

export interface S3ObjectStoreOptions {
  client: S3Like;
  buckets: Record<BucketName, string>;
}

function statusOf(e: unknown): number | undefined {
  return (e as { $metadata?: { httpStatusCode?: number } }).$metadata?.httpStatusCode;
}

function isMissing(e: unknown): boolean {
  const name = (e as { name?: string }).name;
  return name === 'NoSuchKey' || name === 'NotFound' || statusOf(e) === 404;
}

/** CopySource is `<bucket>/<key>` with each key path segment URL-encoded. */
export function copySource(bucket: string, key: string): string {
  return `${bucket}/${key.split('/').map(encodeURIComponent).join('/')}`;
}

export class S3ObjectStore implements ObjectStore {
  private readonly client: S3Like;
  private readonly buckets: Record<BucketName, string>;

  constructor(opts: S3ObjectStoreOptions) {
    this.client = opts.client;
    this.buckets = opts.buckets;
  }

  async put(bucket: BucketName, key: string, body: Uint8Array | string): Promise<void> {
    await this.client.send(new PutObjectCommand({ Bucket: this.buckets[bucket], Key: key, Body: body }));
  }

  async get(bucket: BucketName, key: string): Promise<Uint8Array> {
    try {
      const r = await this.client.send(new GetObjectCommand({ Bucket: this.buckets[bucket], Key: key }));
      return await r.Body.transformToByteArray();
    } catch (e) {
      if (isMissing(e)) throw new NotFoundError(`${bucket}/${key} not found`);
      throw e;
    }
  }

  async getStream(bucket: BucketName, key: string): Promise<NodeJS.ReadableStream> {
    try {
      const r = await this.client.send(new GetObjectCommand({ Bucket: this.buckets[bucket], Key: key }));
      return r.Body instanceof Readable ? r.Body : Readable.from(r.Body);
    } catch (e) {
      if (isMissing(e)) throw new NotFoundError(`${bucket}/${key} not found`);
      throw e;
    }
  }

  async head(bucket: BucketName, key: string): Promise<{ size: number } | undefined> {
    try {
      const r = await this.client.send(new HeadObjectCommand({ Bucket: this.buckets[bucket], Key: key }));
      return { size: Number(r.ContentLength ?? 0) };
    } catch (e) {
      if (isMissing(e)) return undefined;
      throw e;
    }
  }

  async list(bucket: BucketName, prefix: string): Promise<string[]> {
    const keys: string[] = [];
    let token: string | undefined;
    do {
      const r = await this.client.send(
        new ListObjectsV2Command({ Bucket: this.buckets[bucket], Prefix: prefix, ContinuationToken: token }),
      );
      for (const o of r.Contents ?? []) if (o.Key !== undefined) keys.push(o.Key);
      token = r.IsTruncated ? r.NextContinuationToken : undefined;
    } while (token);
    return keys.sort();
  }

  async delete(bucket: BucketName, key: string): Promise<void> {
    await this.client.send(new DeleteObjectCommand({ Bucket: this.buckets[bucket], Key: key }));
  }

  async move(bucket: BucketName, fromKey: string, toKey: string): Promise<void> {
    if (fromKey === toKey) return;
    const b = this.buckets[bucket];
    if (!(await this.head(bucket, fromKey))) {
      if (await this.head(bucket, toKey)) return;
      throw new NotFoundError(`${bucket}/${fromKey} not found`);
    }
    await this.client.send(new CopyObjectCommand({ Bucket: b, Key: toKey, CopySource: copySource(b, fromKey) }));
    await this.client.send(new DeleteObjectCommand({ Bucket: b, Key: fromKey }));
  }
}
