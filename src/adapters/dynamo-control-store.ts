import {
  BatchGetCommand,
  BatchWriteCommand,
  GetCommand,
  PutCommand,
  QueryCommand,
  TransactWriteCommand,
  UpdateCommand,
} from '@aws-sdk/lib-dynamodb';
import {
  NotFoundError,
  StateError,
  type ChunkRecord,
  type ChunkResult,
  type ControlStore,
  type FileMeta,
  type FileStatus,
  type ReplayEdge,
  type RowState,
} from '../ports.js';

/** The slice of DynamoDBDocumentClient this adapter needs. */
export interface DocLike {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  send(command: any): Promise<any>;
}

export interface DynamoControlStoreOptions {
  doc: DocLike;
  table: string;
  /** Replaces the retry sleep in tests. */
  sleep?: (ms: number) => Promise<void>;
}

const pad = (n: number, w: number) => String(n).padStart(w, '0');
/** A replay edge is split into parts of this many rows so one item stays far below the 400 KB item limit. */
const EDGE_PART_ROWS = 2000;
const MAX_BATCH_TRIES = 5;

const fileKey = (sha: string) => `FILE#${sha}`;

function errName(e: unknown): string | undefined {
  return (e as { name?: string }).name;
}

function clean<T extends object>(o: T): T {
  return Object.fromEntries(Object.entries(o).filter(([, v]) => v !== undefined)) as T;
}

function stripKeys<T>(item: Record<string, unknown>): T {
  const { pk: _pk, sk: _sk, ...rest } = item;
  return rest as T;
}

export class DynamoControlStore implements ControlStore {
  private readonly doc: DocLike;
  private readonly table: string;
  private readonly sleep: (ms: number) => Promise<void>;

  constructor(opts: DynamoControlStoreOptions) {
    this.doc = opts.doc;
    this.table = opts.table;
    this.sleep = opts.sleep ?? ((ms) => new Promise((r) => setTimeout(r, ms)));
  }

  private async queryAll(pk: string, skPrefix: string, forward = true): Promise<Array<Record<string, unknown>>> {
    const items: Array<Record<string, unknown>> = [];
    let start: Record<string, unknown> | undefined;
    do {
      const r = await this.doc.send(
        new QueryCommand({
          TableName: this.table,
          KeyConditionExpression: 'pk = :pk AND begins_with(sk, :sk)',
          ExpressionAttributeValues: { ':pk': pk, ':sk': skPrefix },
          ScanIndexForward: forward,
          ExclusiveStartKey: start,
        }),
      );
      items.push(...((r.Items ?? []) as Array<Record<string, unknown>>));
      start = r.LastEvaluatedKey;
    } while (start);
    return items;
  }

  async createFile(meta: FileMeta): Promise<'created' | 'exists'> {
    try {
      await this.doc.send(
        new TransactWriteCommand({
          TransactItems: [
            {
              Put: {
                TableName: this.table,
                Item: { pk: fileKey(meta.sha), sk: 'META', ...clean(meta) },
                ConditionExpression: 'attribute_not_exists(pk)',
              },
            },
            {
              Put: {
                TableName: this.table,
                Item: { pk: `DATASET#${meta.dataset}`, sk: `FILE#${meta.createdAt}#${meta.sha}`, sha: meta.sha },
              },
            },
          ],
        }),
      );
      return 'created';
    } catch (e) {
      if (errName(e) === 'TransactionCanceledException') {
        const reasons = (e as { CancellationReasons?: Array<{ Code?: string }> }).CancellationReasons;
        if (!reasons || reasons[0]?.Code === 'ConditionalCheckFailed') return 'exists';
      }
      throw e;
    }
  }

  async getFile(sha: string): Promise<FileMeta | undefined> {
    const r = await this.doc.send(new GetCommand({ TableName: this.table, Key: { pk: fileKey(sha), sk: 'META' } }));
    return r.Item ? stripKeys<FileMeta>(r.Item) : undefined;
  }

  async updateFile(sha: string, patch: Partial<FileMeta>, opts?: { ifStatus?: FileStatus }): Promise<void> {
    const names: Record<string, string> = {};
    const values: Record<string, unknown> = {};
    let condition = 'attribute_exists(pk)';
    if (opts?.ifStatus) {
      names['#ifStatus'] = 'status';
      values[':ifStatus'] = opts.ifStatus;
      condition += ' AND #ifStatus = :ifStatus';
    }
    const sets: string[] = [];
    const removes: string[] = [];
    Object.entries(patch).forEach(([k, v], i) => {
      names[`#f${i}`] = k;
      if (v === undefined) {
        removes.push(`#f${i}`);
      } else {
        values[`:v${i}`] = v;
        sets.push(`#f${i} = :v${i}`);
      }
    });
    if (sets.length === 0 && removes.length === 0) return;
    const expr = [sets.length ? `SET ${sets.join(', ')}` : '', removes.length ? `REMOVE ${removes.join(', ')}` : '']
      .filter(Boolean)
      .join(' ');
    try {
      await this.doc.send(
        new UpdateCommand({
          TableName: this.table,
          Key: { pk: fileKey(sha), sk: 'META' },
          UpdateExpression: expr,
          ConditionExpression: condition,
          ExpressionAttributeNames: names,
          ...(Object.keys(values).length ? { ExpressionAttributeValues: values } : {}),
        }),
      );
    } catch (e) {
      if (errName(e) === 'ConditionalCheckFailedException') {
        const current = opts?.ifStatus ? await this.getFile(sha) : undefined;
        if (current) throw new StateError(`file ${sha} is ${current.status}, expected ${opts?.ifStatus}`);
        throw new NotFoundError(`no file with sha ${sha}`);
      }
      throw e;
    }
  }

  async listFiles(dataset: string): Promise<FileMeta[]> {
    const index = await this.queryAll(`DATASET#${dataset}`, 'FILE#', false);
    const shas = index.map((i) => String(i.sha));
    const byBatch: FileMeta[] = [];
    const found = new Map<string, FileMeta>();
    for (let i = 0; i < shas.length; i += 100) {
      let keys: Array<{ pk: string; sk: string }> | undefined = shas.slice(i, i + 100).map((s) => ({ pk: fileKey(s), sk: 'META' }));
      for (let tries = 0; keys && keys.length > 0; tries++) {
        const r: { Responses?: Record<string, Array<Record<string, unknown>>>; UnprocessedKeys?: Record<string, { Keys: typeof keys }> } =
          await this.doc.send(new BatchGetCommand({ RequestItems: { [this.table]: { Keys: keys } } }));
        for (const it of r.Responses?.[this.table] ?? []) {
          const m = stripKeys<FileMeta>(it);
          found.set(m.sha, m);
        }
        keys = r.UnprocessedKeys?.[this.table]?.Keys;
        if (keys && keys.length > 0) {
          if (tries >= MAX_BATCH_TRIES) throw new Error('BatchGet left unprocessed keys after retries');
          await this.sleep(20 * 2 ** tries);
        }
      }
    }
    for (const s of shas) {
      const m = found.get(s);
      if (m) byBatch.push(m);
    }
    return byBatch;
  }

  async putChunk(sha: string, chunk: ChunkRecord): Promise<void> {
    try {
      await this.doc.send(
        new PutCommand({
          TableName: this.table,
          Item: { pk: fileKey(sha), sk: `CHUNK#${pad(chunk.index, 5)}`, ...clean(chunk) },
          ConditionExpression: 'attribute_not_exists(pk)',
        }),
      );
    } catch (e) {
      if (errName(e) !== 'ConditionalCheckFailedException') throw e;
    }
  }

  async recordChunkResult(sha: string, index: number, r: ChunkResult): Promise<'applied' | 'stale'> {
    try {
      await this.doc.send(
        new UpdateCommand({
          TableName: this.table,
          Key: { pk: fileKey(sha), sk: `CHUNK#${pad(index, 5)}` },
          UpdateExpression: 'SET #v = :v, #q = :q, #o = :o, #k = :k, #a = :a',
          ConditionExpression: 'attribute_exists(pk) AND (attribute_not_exists(#a) OR #a <= :a)',
          ExpressionAttributeNames: {
            '#v': 'rowsValid',
            '#q': 'rowsQuarantined',
            '#o': 'outputKey',
            '#k': 'quarantineKey',
            '#a': 'attempt',
          },
          ExpressionAttributeValues: {
            ':v': r.rowsValid,
            ':q': r.rowsQuarantined,
            ':o': r.outputKey,
            ':k': r.quarantineKey,
            ':a': r.attempt,
          },
        }),
      );
      return 'applied';
    } catch (e) {
      if (errName(e) === 'ConditionalCheckFailedException') return 'stale';
      throw e;
    }
  }

  async listChunks(sha: string): Promise<ChunkRecord[]> {
    const items = await this.queryAll(fileKey(sha), 'CHUNK#');
    return items.map((i) => stripKeys<ChunkRecord>(i)).sort((a, b) => a.index - b.index);
  }

  async resetChunks(sha: string): Promise<void> {
    const keys = (await this.queryAll(fileKey(sha), 'CHUNK#')).map((i) => ({ pk: i.pk as string, sk: i.sk as string }));
    for (let i = 0; i < keys.length; i += 25) {
      let requests: Array<{ DeleteRequest: { Key: { pk: string; sk: string } } }> | undefined = keys
        .slice(i, i + 25)
        .map((Key) => ({ DeleteRequest: { Key } }));
      for (let tries = 0; requests && requests.length > 0; tries++) {
        const r: { UnprocessedItems?: Record<string, typeof requests> } = await this.doc.send(
          new BatchWriteCommand({ RequestItems: { [this.table]: requests } }),
        );
        requests = r.UnprocessedItems?.[this.table];
        if (requests && requests.length > 0) {
          if (tries >= MAX_BATCH_TRIES) throw new Error('BatchWrite left unprocessed items after retries');
          await this.sleep(20 * 2 ** tries);
        }
      }
    }
  }

  async putReplayEdge(parentSha: string, edge: ReplayEdge): Promise<void> {
    const parts = Math.max(1, Math.ceil(edge.parentRows.length / EDGE_PART_ROWS));
    for (let p = 0; p < parts; p++) {
      const from = p * EDGE_PART_ROWS;
      const to = from + EDGE_PART_ROWS;
      await this.doc.send(
        new PutCommand({
          TableName: this.table,
          Item: clean({
            pk: fileKey(parentSha),
            sk: `REPLAY#${edge.childSha}#${pad(p, 4)}`,
            childSha: edge.childSha,
            childKey: edge.childKey,
            createdAt: edge.createdAt,
            part: p,
            parts,
            parentRows: edge.parentRows.slice(from, to),
            rowHashes: edge.rowHashes?.slice(from, to),
          }),
        }),
      );
    }
  }

  async listReplayEdges(parentSha: string): Promise<ReplayEdge[]> {
    const items = await this.queryAll(fileKey(parentSha), 'REPLAY#');
    const byChild = new Map<string, ReplayEdge>();
    for (const it of items.sort((a, b) => Number(a.part) - Number(b.part))) {
      const childSha = String(it.childSha);
      const rows = (it.parentRows ?? []) as number[];
      const hashes = it.rowHashes as string[] | undefined;
      const cur = byChild.get(childSha);
      if (!cur) {
        byChild.set(childSha, {
          childSha,
          childKey: String(it.childKey),
          createdAt: String(it.createdAt),
          parentRows: [...rows],
          ...(hashes ? { rowHashes: [...hashes] } : {}),
        });
      } else {
        cur.parentRows.push(...rows);
        if (hashes) (cur.rowHashes ??= []).push(...hashes);
      }
    }
    return [...byChild.values()].sort((a, b) => a.createdAt.localeCompare(b.createdAt) || a.childSha.localeCompare(b.childSha));
  }

  async putRowStates(parentSha: string, entries: Array<[number, RowState]>): Promise<void> {
    const last = new Map<number, RowState>();
    for (const [row, state] of entries) last.set(row, state);
    const all = [...last.entries()];
    for (let i = 0; i < all.length; i += 25) {
      let requests: Array<{ PutRequest: { Item: Record<string, unknown> } }> | undefined = all.slice(i, i + 25).map(([row, state]) => ({
        PutRequest: { Item: { pk: fileKey(parentSha), sk: `ROW#${pad(row, 10)}`, row, state } },
      }));
      for (let tries = 0; requests && requests.length > 0; tries++) {
        const r: { UnprocessedItems?: Record<string, typeof requests> } = await this.doc.send(
          new BatchWriteCommand({ RequestItems: { [this.table]: requests } }),
        );
        requests = r.UnprocessedItems?.[this.table];
        if (requests && requests.length > 0) {
          if (tries >= MAX_BATCH_TRIES) throw new Error('BatchWrite left unprocessed items after retries');
          await this.sleep(20 * 2 ** tries);
        }
      }
    }
  }

  async listRowStates(parentSha: string): Promise<Map<number, RowState>> {
    const out = new Map<number, RowState>();
    for (const it of await this.queryAll(fileKey(parentSha), 'ROW#')) out.set(Number(it.row), it.state as RowState);
    return out;
  }

  async recordDuplicate(dataset: string, sha: string, key: string, at: string): Promise<void> {
    await this.doc.send(
      new PutCommand({
        TableName: this.table,
        Item: { pk: `DATASET#${dataset}`, sk: `DUP#${at}#${sha}`, sha, key, at },
      }),
    );
  }
}
