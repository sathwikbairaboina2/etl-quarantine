import { KeyError, datasetFromRawKey, parentShaFromReplayKey } from '../core/keys.js';
import { LimitError, checkFileSize } from '../core/limits.js';
import { UnknownDatasetError, getDataset, type Dataset } from '../datasets/registry.js';
import { NotFoundError, type FileMeta } from '../ports.js';
import type { Deps } from './deps.js';
import { sha256Stream } from './hash.js';

export interface RegisterResult {
  status: 'REGISTERED' | 'DUPLICATE' | 'FAILED';
  sha: string;
  dataset: string;
  key: string;
  originalKey?: string;
  error?: string;
}

export async function register(deps: Deps, input: { key: string }): Promise<RegisterResult> {
  const { key } = input;
  const head = await deps.objects.head('raw', key);
  if (!head) throw new NotFoundError(`raw/${key} not found`);

  let dataset = '';
  let ds: Dataset;
  try {
    checkFileSize(head.size, deps.limits);
    dataset = datasetFromRawKey(key);
    ds = getDataset(dataset);
  } catch (e) {
    if (e instanceof LimitError || e instanceof KeyError || e instanceof UnknownDatasetError) {
      return { status: 'FAILED', sha: '', dataset, key, error: e.message };
    }
    throw e;
  }

  const sha = await sha256Stream(await deps.objects.getStream('raw', key));
  const at = deps.now().toISOString();
  const parentSha = parentShaFromReplayKey(key);
  const meta: FileMeta = {
    sha,
    dataset,
    sourceKey: key,
    schemaVersion: ds.manifest.schema,
    status: 'REGISTERED',
    columns: [],
    ingestDate: at.slice(0, 10),
    createdAt: at,
    updatedAt: at,
    ...(parentSha ? { parentSha } : {}),
  };
  if ((await deps.control.createFile(meta)) === 'created') {
    return { status: 'REGISTERED', sha, dataset, key };
  }
  const existing = await deps.control.getFile(sha);
  await deps.control.recordDuplicate(dataset, sha, key, at);
  return { status: 'DUPLICATE', sha, dataset, key, ...(existing ? { originalKey: existing.sourceKey } : {}) };
}
