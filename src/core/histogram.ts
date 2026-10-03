import type { QuarantineRecord } from './types.js';

export interface HistogramRow {
  instancePath: string;
  keyword: string;
  count: number;
}

/** Counts errors by (instancePath, keyword), count desc then path then keyword. */
export function histogram(records: Pick<QuarantineRecord, 'errors'>[]): HistogramRow[] {
  const counts = new Map<string, HistogramRow>();
  for (const r of records) {
    for (const e of r.errors) {
      const k = `${e.instancePath}\u0000${e.keyword}`;
      const row = counts.get(k);
      if (row) row.count++;
      else counts.set(k, { instancePath: e.instancePath, keyword: e.keyword, count: 1 });
    }
  }
  return [...counts.values()].sort(
    (a, b) => b.count - a.count || a.instancePath.localeCompare(b.instancePath) || a.keyword.localeCompare(b.keyword),
  );
}
