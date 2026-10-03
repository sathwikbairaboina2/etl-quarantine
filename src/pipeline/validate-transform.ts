import { checkChunk } from '../core/accounting.js';
import { fieldsToRecord } from '../core/csv.js';
import { curatedPendingKey, quarantineChunkKey } from '../core/keys.js';
import { normalize } from '../core/normalize.js';
import { InvariantError, type QuarantineRecord, type ValidRow } from '../core/types.js';
import { createValidator, validateRow } from '../core/validate.js';
import { getDataset } from '../datasets/registry.js';
import { NotFoundError, type ChunkResult } from '../ports.js';
import type { Deps } from './deps.js';
import { writeParquet } from './parquet.js';

export interface ValidateTransformInput {
  sha: string;
  chunk: { index: number; key: string; rows: number; sha?: string };
  attempt: number;
}

export type ValidateTransformResult = ChunkResult & { index: number; rowsIn: number };

interface StagedLine {
  rowNumber: number;
  raw: string;
  fields: string[];
}

export async function validateTransform(deps: Deps, input: ValidateTransformInput): Promise<ValidateTransformResult> {
  const { sha, chunk, attempt } = input;
  const meta = await deps.control.getFile(sha);
  if (!meta) throw new NotFoundError(`no file with sha ${sha}`);
  const { manifest, schema } = getDataset(meta.dataset);
  const validator = createValidator(schema);

  const text = new TextDecoder().decode(await deps.objects.get('staging', chunk.key));
  const lines = text
    .split('\n')
    .filter((l) => l !== '')
    .map((l) => JSON.parse(l) as StagedLine);

  const valid: ValidRow[] = [];
  const quarantined: QuarantineRecord[] = [];
  const quarantinedAt = deps.now().toISOString();
  for (const line of lines) {
    const { record, errors } = fieldsToRecord(line.fields, meta.columns);
    const parsed = normalize(record, manifest.columns);
    let rowErrors = errors;
    if (errors.length === 0) {
      const r = validateRow(validator, parsed);
      if (r.ok) {
        valid.push(r.row);
        continue;
      }
      rowErrors = r.errors;
    }
    quarantined.push({
      fileSha: sha,
      chunk: chunk.index,
      rowNumber: line.rowNumber,
      raw: line.raw,
      parsed,
      schemaVersion: meta.schemaVersion,
      errors: rowErrors,
      quarantinedAt,
    });
  }

  deps.faults?.beforeOutput?.(chunk.index, attempt);

  // Keys use the file's ingestDate, never now(), so a retry rewrites the same objects.
  let outputKey: string | null = null;
  if (valid.length > 0) {
    outputKey = curatedPendingKey(meta.dataset, meta.ingestDate, sha, chunk.index);
    await deps.objects.put('curated', outputKey, writeParquet(valid, manifest.columns));
  }
  let quarantineKey: string | null = null;
  if (quarantined.length > 0) {
    quarantineKey = quarantineChunkKey(meta.dataset, sha, chunk.index);
    await deps.objects.put('quarantine', quarantineKey, `${quarantined.map((q) => JSON.stringify(q)).join('\n')}\n`);
  }

  const counts = { index: chunk.index, rowsIn: chunk.rows, rowsValid: valid.length, rowsQuarantined: quarantined.length };
  const check = checkChunk(counts);
  if (!check.ok) throw new InvariantError(check.reason);
  if (lines.length !== chunk.rows) {
    throw new InvariantError(`chunk ${chunk.index}: staged ${lines.length} rows but split recorded ${chunk.rows}`);
  }

  deps.faults?.afterOutput?.(chunk.index, attempt);

  const result: ChunkResult = {
    rowsValid: valid.length,
    rowsQuarantined: quarantined.length,
    outputKey,
    quarantineKey,
    attempt,
  };
  await deps.control.recordChunkResult(sha, chunk.index, result);
  return { ...result, index: chunk.index, rowsIn: chunk.rows };
}
