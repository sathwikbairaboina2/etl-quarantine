import { randomBytes } from 'node:crypto';
import { CreateBucketCommand, S3Client } from '@aws-sdk/client-s3';
import { CreateTableCommand, DynamoDBClient, ListTablesCommand } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { DynamoControlStore } from '../../src/adapters/dynamo-control-store.js';
import { S3ObjectStore } from '../../src/adapters/s3-object-store.js';
import { DEFAULT_LIMITS } from '../../src/core/limits.js';
import type { Deps } from '../../src/pipeline/deps.js';
import type { BucketName } from '../../src/ports.js';
import { fixedClock } from '../support/clock.js';

export const DDB_ENDPOINT = process.env.ETL_IT_DDB_ENDPOINT ?? 'http://127.0.0.1:5340';
export const S3_ENDPOINT = process.env.ETL_IT_S3_ENDPOINT ?? 'http://127.0.0.1:5341';
const credentials = { accessKeyId: 'test', secretAccessKey: 'test' };

export interface AwsEnv {
  objects: S3ObjectStore;
  control: DynamoControlStore;
  table: string;
  buckets: Record<BucketName, string>;
}

let ready: Promise<void> | undefined;

/** Waits for both emulators; cold starts take 30 s or more (S3Mock about 30 s, DynamoDB Local about 32 s measured). ETL_IT_WAIT_MS overrides the 120 s default. */
function waitForEmulators(): Promise<void> {
  ready ??= (async () => {
    const ddb = new DynamoDBClient({ endpoint: DDB_ENDPOINT, region: 'us-east-1', credentials });
    const s3 = new S3Client({ endpoint: S3_ENDPOINT, region: 'us-east-1', credentials, forcePathStyle: true });
    const deadline = Date.now() + Number(process.env.ETL_IT_WAIT_MS ?? 120_000);
    let lastError: unknown;
    while (Date.now() < deadline) {
      try {
        await ddb.send(new ListTablesCommand({}));
        await s3.send(new CreateBucketCommand({ Bucket: `etlq-probe-${randomBytes(4).toString('hex')}` }));
        return;
      } catch (e) {
        lastError = e;
        await new Promise((r) => setTimeout(r, 500));
      }
    }
    throw new Error(`emulators not reachable at ${DDB_ENDPOINT} / ${S3_ENDPOINT}: ${(lastError as Error)?.message}`);
  })();
  return ready;
}

/** A fresh table and four fresh buckets, so tests cannot see each other's data. */
export async function newAwsEnv(): Promise<AwsEnv> {
  await waitForEmulators();
  const id = randomBytes(5).toString('hex');
  const ddb = new DynamoDBClient({ endpoint: DDB_ENDPOINT, region: 'us-east-1', credentials });
  const s3 = new S3Client({ endpoint: S3_ENDPOINT, region: 'us-east-1', credentials, forcePathStyle: true });
  const table = `etlq-${id}`;
  await ddb.send(
    new CreateTableCommand({
      TableName: table,
      BillingMode: 'PAY_PER_REQUEST',
      AttributeDefinitions: [
        { AttributeName: 'pk', AttributeType: 'S' },
        { AttributeName: 'sk', AttributeType: 'S' },
      ],
      KeySchema: [
        { AttributeName: 'pk', KeyType: 'HASH' },
        { AttributeName: 'sk', KeyType: 'RANGE' },
      ],
    }),
  );
  const buckets = {
    raw: `etlq-${id}-raw`,
    staging: `etlq-${id}-staging`,
    curated: `etlq-${id}-curated`,
    quarantine: `etlq-${id}-quarantine`,
  } as Record<BucketName, string>;
  for (const b of Object.values(buckets)) await s3.send(new CreateBucketCommand({ Bucket: b }));
  return {
    objects: new S3ObjectStore({ client: s3, buckets }),
    control: new DynamoControlStore({ doc: DynamoDBDocumentClient.from(ddb), table }),
    table,
    buckets,
  };
}

export async function awsDeps(over: Partial<Deps> = {}): Promise<Deps & { env: AwsEnv }> {
  const env = await newAwsEnv();
  return { objects: env.objects, control: env.control, now: fixedClock, limits: { ...DEFAULT_LIMITS }, env, ...over };
}
