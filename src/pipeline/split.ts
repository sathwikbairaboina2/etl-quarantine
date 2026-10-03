import { CsvError, parse } from 'csv-parse';
import { HeaderError, checkHeader } from '../core/csv.js';
import { stagingChunkKey, stagingManifestKey } from '../core/keys.js';
import { LimitError, checkColumns } from '../core/limits.js';
import { getDataset } from '../datasets/registry.js';
import { NotFoundError } from '../ports.js';
import type { Deps } from './deps.js';

export interface ChunkItem {
  sha: string;
  index: number;
  key: string;
  rows: number;
}

export interface SplitResult {
  status: 'SPLIT' | 'FAILED';
  sha: string;
  chunkCount: number;
  rowsIn: number;
  manifestKey: string;
  error?: string;
}

interface Parsed {
  record: string[];
  raw: string;
}

/** Streams the raw file into staging chunks of `manifest.chunkRows` rows plus an item list for the Map state. */
export async function split(deps: Deps, input: { sha: string }): Promise<SplitResult> {
  const { sha } = input;
  const meta = await deps.control.getFile(sha);
  if (!meta) throw new NotFoundError(`no file with sha ${sha}`);
  const { manifest } = getDataset(meta.dataset);
  const chunkRows = deps.chunkRows ?? manifest.chunkRows;
  const manifestKey = stagingManifestKey(sha);
  const written: string[] = [];
  const items: ChunkItem[] = [];

  const fail = async (error: string): Promise<SplitResult> => {
    for (const k of written) await deps.objects.delete('staging', k);
    await deps.objects.delete('staging', manifestKey);
    await deps.control.updateFile(sha, { status: 'FAILED', error, updatedAt: deps.now().toISOString() });
    return { status: 'FAILED', sha, chunkCount: 0, rowsIn: 0, manifestKey, error };
  };

  try {
    const source = await deps.objects.getStream('raw', meta.sourceKey);
    const parser = parse({
      bom: true,
      raw: true,
      info: true,
      relax_column_count: true,
      skip_empty_lines: true,
      max_record_size: deps.limits.maxRowBytes,
    });
    source.on('error', (e) => parser.destroy(e));
    source.pipe(parser);

    let header: string[] | undefined;
    let rowNumber = 0;
    let lines: string[] = [];

    const flush = async () => {
      if (lines.length === 0) return;
      const index = items.length;
      const key = stagingChunkKey(sha, index);
      await deps.objects.put('staging', key, `${lines.join('\n')}\n`);
      written.push(key);
      items.push({ sha, index, key, rows: lines.length });
      lines = [];
    };

    for await (const item of parser as AsyncIterable<{ record: string[]; raw: string } & Partial<Parsed>>) {
      if (!header) {
        checkColumns(item.record.length, deps.limits);
        header = checkHeader(item.record, manifest);
        continue;
      }
      rowNumber++;
      const raw = (item.raw ?? '').replace(/[\r\n]+$/, '');
      lines.push(JSON.stringify({ rowNumber, raw, fields: item.record }));
      if (lines.length >= chunkRows) await flush();
    }
    if (!header) throw new HeaderError('file is empty: no header row');
    await flush();

    for (const it of items) {
      await deps.control.putChunk(sha, { index: it.index, key: it.key, rowsIn: it.rows });
    }
    await deps.objects.put('staging', manifestKey, JSON.stringify(items));
    await deps.control.updateFile(sha, {
      status: 'SPLIT',
      columns: header,
      rowsIn: rowNumber,
      chunkCount: items.length,
      updatedAt: deps.now().toISOString(),
    });
    return { status: 'SPLIT', sha, chunkCount: items.length, rowsIn: rowNumber, manifestKey };
  } catch (e) {
    if (e instanceof CsvError || e instanceof LimitError || e instanceof HeaderError) {
      return fail(e.message);
    }
    throw e;
  }
}
