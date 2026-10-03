import { describe, expect, it } from 'vitest';
import { DynamoControlStore } from '../../src/adapters/dynamo-control-store.js';
import { S3ObjectStore, copySource } from '../../src/adapters/s3-object-store.js';
import { NotFoundError, type FileMeta } from '../../src/ports.js';

interface Call {
  name: string;
  input: any;
}

/** A hand-written send() that records commands and answers from a queue of responders. */
function fake(respond: (c: Call, n: number) => any) {
  const calls: Call[] = [];
  return {
    calls,
    client: {
      async send(cmd: any) {
        const c = { name: cmd.constructor.name, input: cmd.input };
        calls.push(c);
        const r = respond(c, calls.length - 1);
        if (r instanceof Error) throw r;
        return r;
      },
    },
  };
}

const awsError = (name: string, extra: object = {}) => Object.assign(new Error(name), { name, ...extra });
const buckets = { raw: 'r', staging: 's', curated: 'c', quarantine: 'q' } as const;

describe('S3ObjectStore (fake client)', () => {
  it('encodes each CopySource path segment', () => {
    expect(copySource('c', '_pending/dataset=customers/ingest_date=2026-10-04/x y.parquet')).toBe(
      'c/_pending/dataset%3Dcustomers/ingest_date%3D2026-10-04/x%20y.parquet',
    );
  });

  it('move heads, copies with the encoded source, then deletes', async () => {
    const f = fake((c) => (c.name === 'HeadObjectCommand' ? { ContentLength: 5 } : {}));
    const s = new S3ObjectStore({ client: f.client, buckets });
    await s.move('curated', '_pending/dataset=customers/ingest_date=2026-10-04/x.parquet', 'dataset=customers/ingest_date=2026-10-04/x.parquet');
    expect(f.calls.map((c) => c.name)).toEqual(['HeadObjectCommand', 'CopyObjectCommand', 'DeleteObjectCommand']);
    expect(f.calls[1]!.input).toEqual({
      Bucket: 'c',
      Key: 'dataset=customers/ingest_date=2026-10-04/x.parquet',
      CopySource: 'c/_pending/dataset%3Dcustomers/ingest_date%3D2026-10-04/x.parquet',
    });
    expect(f.calls[2]!.input).toEqual({ Bucket: 'c', Key: '_pending/dataset=customers/ingest_date=2026-10-04/x.parquet' });
  });

  it('move is a no-op when only the destination exists and throws when neither does', async () => {
    const onlyTo = fake((c) => (c.input.Key === 'to' ? { ContentLength: 1 } : awsError('NotFound', { $metadata: { httpStatusCode: 404 } })));
    await new S3ObjectStore({ client: onlyTo.client, buckets }).move('curated', 'from', 'to');
    expect(onlyTo.calls.map((c) => c.name)).toEqual(['HeadObjectCommand', 'HeadObjectCommand']);

    const neither = fake(() => awsError('NotFound', { $metadata: { httpStatusCode: 404 } }));
    await expect(new S3ObjectStore({ client: neither.client, buckets }).move('curated', 'a', 'b')).rejects.toBeInstanceOf(NotFoundError);
  });

  it('list follows continuation tokens across 2 pages and sorts', async () => {
    const f = fake((_c, n) =>
      n === 0
        ? { Contents: [{ Key: 'b' }, { Key: 'c' }], IsTruncated: true, NextContinuationToken: 't1' }
        : { Contents: [{ Key: 'a' }], IsTruncated: false },
    );
    const keys = await new S3ObjectStore({ client: f.client, buckets }).list('raw', 'p/');
    expect(keys).toEqual(['a', 'b', 'c']);
    expect(f.calls[0]!.input).toMatchObject({ Bucket: 'r', Prefix: 'p/' });
    expect(f.calls[1]!.input.ContinuationToken).toBe('t1');
  });

  it('maps NoSuchKey to NotFoundError and 404 head to undefined', async () => {
    const f = fake((c) => (c.name === 'GetObjectCommand' ? awsError('NoSuchKey') : awsError('NotFound', { $metadata: { httpStatusCode: 404 } })));
    const s = new S3ObjectStore({ client: f.client, buckets });
    await expect(s.get('raw', 'k')).rejects.toBeInstanceOf(NotFoundError);
    expect(await s.head('raw', 'k')).toBeUndefined();
  });

  it('rethrows other errors', async () => {
    const f = fake(() => awsError('AccessDenied', { $metadata: { httpStatusCode: 403 } }));
    await expect(new S3ObjectStore({ client: f.client, buckets }).head('raw', 'k')).rejects.toThrow('AccessDenied');
  });
});

const meta: FileMeta = {
  sha: 'abc',
  dataset: 'customers',
  sourceKey: 'dataset=customers/a.csv',
  schemaVersion: 'customers/v1',
  status: 'REGISTERED',
  columns: [],
  ingestDate: '2026-10-04',
  createdAt: '2026-10-04T09:00:00.000Z',
  updatedAt: '2026-10-04T09:00:00.000Z',
};

describe('DynamoControlStore (fake client)', () => {
  it('createFile writes META with a not-exists condition plus the dataset index in one transaction', async () => {
    const f = fake(() => ({}));
    const c = new DynamoControlStore({ doc: f.client, table: 'T' });
    expect(await c.createFile(meta)).toBe('created');
    const items = f.calls[0]!.input.TransactItems;
    expect(items[0].Put.ConditionExpression).toBe('attribute_not_exists(pk)');
    expect(items[0].Put.Item).toMatchObject({ pk: 'FILE#abc', sk: 'META', sha: 'abc' });
    expect(items[1].Put.Item).toEqual({ pk: 'DATASET#customers', sk: 'FILE#2026-10-04T09:00:00.000Z#abc', sha: 'abc' });
  });

  it('createFile reports exists when the META condition fails', async () => {
    const f = fake(() => awsError('TransactionCanceledException', { CancellationReasons: [{ Code: 'ConditionalCheckFailed' }, { Code: 'None' }] }));
    expect(await new DynamoControlStore({ doc: f.client, table: 'T' }).createFile(meta)).toBe('exists');
  });

  it('createFile rethrows other cancellation reasons', async () => {
    const f = fake(() => awsError('TransactionCanceledException', { CancellationReasons: [{ Code: 'None' }, { Code: 'ProvisionedThroughputExceeded' }] }));
    await expect(new DynamoControlStore({ doc: f.client, table: 'T' }).createFile(meta)).rejects.toThrow();
  });

  it('putChunk is conditional and swallows the failed condition', async () => {
    const f = fake(() => awsError('ConditionalCheckFailedException'));
    const c = new DynamoControlStore({ doc: f.client, table: 'T' });
    await c.putChunk('abc', { index: 42, key: 'k', rowsIn: 5 });
    expect(f.calls[0]!.input.ConditionExpression).toBe('attribute_not_exists(pk)');
    expect(f.calls[0]!.input.Item).toMatchObject({ pk: 'FILE#abc', sk: 'CHUNK#00042', index: 42 });
  });

  it('recordChunkResult uses the attempt-guarded condition and maps failure to stale', async () => {
    const ok = fake(() => ({}));
    const c = new DynamoControlStore({ doc: ok.client, table: 'T' });
    const r = { rowsValid: 1, rowsQuarantined: 2, outputKey: 'o', quarantineKey: null, attempt: 1 };
    expect(await c.recordChunkResult('abc', 3, r)).toBe('applied');
    expect(ok.calls[0]!.input.ConditionExpression).toBe('attribute_exists(pk) AND (attribute_not_exists(#a) OR #a <= :a)');
    expect(ok.calls[0]!.input.Key).toEqual({ pk: 'FILE#abc', sk: 'CHUNK#00003' });
    expect(ok.calls[0]!.input.ExpressionAttributeValues[':a']).toBe(1);

    const stale = fake(() => awsError('ConditionalCheckFailedException'));
    expect(await new DynamoControlStore({ doc: stale.client, table: 'T' }).recordChunkResult('abc', 3, r)).toBe('stale');
  });

  it('updateFile maps a failed condition to NotFoundError and aliases reserved words', async () => {
    const f = fake(() => awsError('ConditionalCheckFailedException'));
    const c = new DynamoControlStore({ doc: f.client, table: 'T' });
    await expect(c.updateFile('abc', { status: 'FAILED', error: 'x' })).rejects.toBeInstanceOf(NotFoundError);
    const input = f.calls[0]!.input;
    expect(input.UpdateExpression).toBe('SET #f0 = :v0, #f1 = :v1');
    expect(input.ExpressionAttributeNames).toEqual({ '#f0': 'status', '#f1': 'error' });
    expect(input.ConditionExpression).toBe('attribute_exists(pk)');
  });

  it('queries paginate on LastEvaluatedKey across 2 pages', async () => {
    const f = fake((_c, n) =>
      n === 0
        ? { Items: [{ pk: 'FILE#abc', sk: 'CHUNK#00000', index: 0, key: 'k0', rowsIn: 1 }], LastEvaluatedKey: { pk: 'x', sk: 'y' } }
        : { Items: [{ pk: 'FILE#abc', sk: 'CHUNK#00001', index: 1, key: 'k1', rowsIn: 1 }] },
    );
    const chunks = await new DynamoControlStore({ doc: f.client, table: 'T' }).listChunks('abc');
    expect(chunks.map((c) => c.index)).toEqual([0, 1]);
    expect(f.calls[1]!.input.ExclusiveStartKey).toEqual({ pk: 'x', sk: 'y' });
    expect(f.calls[0]!.input.ExpressionAttributeValues).toEqual({ ':pk': 'FILE#abc', ':sk': 'CHUNK#' });
  });

  it('putRowStates batches in groups of 25 and retries UnprocessedItems once', async () => {
    let first = true;
    const f = fake((c) => {
      const items = c.input.RequestItems.T;
      if (first) {
        first = false;
        return { UnprocessedItems: { T: items.slice(0, 3) } };
      }
      return {};
    });
    const sleeps: number[] = [];
    const c = new DynamoControlStore({ doc: f.client, table: 'T', sleep: async (ms) => void sleeps.push(ms) });
    await c.putRowStates('abc', Array.from({ length: 30 }, (_, i): [number, 'pending'] => [i + 1, 'pending']));
    const sizes = f.calls.map((x) => x.input.RequestItems.T.length);
    expect(sizes).toEqual([25, 3, 5]);
    expect(sleeps).toEqual([20]);
    expect(f.calls[0]!.input.RequestItems.T[0].PutRequest.Item).toEqual({ pk: 'FILE#abc', sk: 'ROW#0000000001', row: 1, state: 'pending' });
  });

  it('putRowStates collapses duplicate rows (BatchWrite rejects duplicate keys)', async () => {
    const f = fake(() => ({}));
    await new DynamoControlStore({ doc: f.client, table: 'T' }).putRowStates('abc', [
      [1, 'pending'],
      [1, 'loaded'],
    ]);
    expect(f.calls[0]!.input.RequestItems.T).toHaveLength(1);
    expect(f.calls[0]!.input.RequestItems.T[0].PutRequest.Item.state).toBe('loaded');
  });

  it('gives up after repeated unprocessed items', async () => {
    const f = fake((c) => ({ UnprocessedItems: { T: c.input.RequestItems.T } }));
    const c = new DynamoControlStore({ doc: f.client, table: 'T', sleep: async () => {} });
    await expect(c.putRowStates('abc', [[1, 'pending']])).rejects.toThrow(/unprocessed/);
  });

  it('splits a large replay edge into parts and merges them back', async () => {
    const store = new Map<string, any>();
    const f = fake((c) => {
      if (c.name === 'PutCommand') {
        store.set(`${c.input.Item.pk}|${c.input.Item.sk}`, c.input.Item);
        return {};
      }
      return { Items: [...store.values()].filter((i) => i.sk.startsWith('REPLAY#')).sort((a, b) => a.sk.localeCompare(b.sk)) };
    });
    const c = new DynamoControlStore({ doc: f.client, table: 'T' });
    const rows = Array.from({ length: 4500 }, (_, i) => i + 1);
    await c.putReplayEdge('abc', { childSha: 'kid', childKey: 'k', parentRows: rows, rowHashes: rows.map(String), createdAt: '2026-10-04T09:00:00.000Z' });
    expect(f.calls.filter((x) => x.name === 'PutCommand')).toHaveLength(3);
    const [edge] = await c.listReplayEdges('abc');
    expect(edge!.parentRows).toEqual(rows);
    expect(edge!.rowHashes).toEqual(rows.map(String));
    expect(edge).toMatchObject({ childSha: 'kid', childKey: 'k' });
  });

  it('listFiles reads the newest-first index then batch-gets META, retrying unprocessed keys', async () => {
    let n = 0;
    const f = fake((c) => {
      if (c.name === 'QueryCommand') {
        return { Items: [{ sha: 'b' }, { sha: 'a' }] };
      }
      n++;
      if (n === 1) {
        return { Responses: { T: [{ pk: 'FILE#b', sk: 'META', ...meta, sha: 'b' }] }, UnprocessedKeys: { T: { Keys: [{ pk: 'FILE#a', sk: 'META' }] } } };
      }
      return { Responses: { T: [{ pk: 'FILE#a', sk: 'META', ...meta, sha: 'a' }] } };
    });
    const files = await new DynamoControlStore({ doc: f.client, table: 'T', sleep: async () => {} }).listFiles('customers');
    expect(files.map((x) => x.sha)).toEqual(['b', 'a']);
    expect(f.calls[0]!.input.ScanIndexForward).toBe(false);
    expect(f.calls[0]!.input.ExpressionAttributeValues).toEqual({ ':pk': 'DATASET#customers', ':sk': 'FILE#' });
  });

  it('recordDuplicate writes an audit item and leaves META alone', async () => {
    const f = fake(() => ({}));
    await new DynamoControlStore({ doc: f.client, table: 'T' }).recordDuplicate('customers', 'abc', 'dataset=customers/copy.csv', '2026-10-04T10:00:00.000Z');
    expect(f.calls).toHaveLength(1);
    expect(f.calls[0]!.input.Item).toMatchObject({ pk: 'DATASET#customers', sk: 'DUP#2026-10-04T10:00:00.000Z#abc' });
  });
});
