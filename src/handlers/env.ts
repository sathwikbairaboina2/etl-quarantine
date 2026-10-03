import { S3Client } from '@aws-sdk/client-s3';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { DynamoControlStore } from '../adapters/dynamo-control-store.js';
import { S3ObjectStore } from '../adapters/s3-object-store.js';
import { DEFAULT_LIMITS } from '../core/limits.js';
import type { Deps } from '../pipeline/deps.js';
import type { BucketName } from '../ports.js';

function need(env: NodeJS.ProcessEnv, name: string): string {
  const v = env[name];
  if (!v) throw new Error(`missing env ${name}`);
  return v;
}

/**
 * Builds the AWS-backed dependencies from Lambda environment variables.
 * ETL_FAULT_AFTER_OUTPUT_CHUNKS (comma list of chunk indexes) makes the first attempt of those chunks
 * crash after writing output. It exists for failure drills only.
 */
export function depsFromEnv(env: NodeJS.ProcessEnv = process.env): Deps {
  const buckets: Record<BucketName, string> = {
    raw: need(env, 'RAW_BUCKET'),
    staging: need(env, 'STAGING_BUCKET'),
    curated: need(env, 'CURATED_BUCKET'),
    quarantine: need(env, 'QUARANTINE_BUCKET'),
  };
  const table = need(env, 'CONTROL_TABLE');
  const drill = new Set((env.ETL_FAULT_AFTER_OUTPUT_CHUNKS ?? '').split(',').filter(Boolean).map(Number));
  return {
    objects: new S3ObjectStore({ client: new S3Client({}), buckets }),
    control: new DynamoControlStore({
      doc: DynamoDBDocumentClient.from(new DynamoDBClient({}), { marshallOptions: { removeUndefinedValues: true } }),
      table,
    }),
    now: () => new Date(),
    limits: { ...DEFAULT_LIMITS },
    ...(drill.size > 0
      ? {
          faults: {
            afterOutput(chunk: number, attempt: number) {
              if (attempt === 0 && drill.has(chunk)) throw new Error(`drill: injected crash after output on chunk ${chunk}`);
            },
          },
        }
      : {}),
  };
}

/** Builds deps once per container. */
export function memoize(make: () => Deps): () => Deps {
  let deps: Deps | undefined;
  return () => (deps ??= make());
}
