import type { ColumnSpec, NormalizeOp, RawRecord } from './types.js';

function applyOp(op: NormalizeOp, v: string | null): string | null {
  if (v === null) return null;
  switch (op) {
    case 'trim':
      return v.trim();
    case 'lowercase':
      return v.toLowerCase();
    case 'uppercase':
      return v.toUpperCase();
    case 'emptyToNull':
      return v === '' ? null : v;
  }
}

/** Applies each column's ops in listed order. Columns not in the record are left untouched. */
export function normalize(record: RawRecord, columns: ColumnSpec[]): RawRecord {
  const out: RawRecord = { ...record };
  for (const col of columns) {
    if (!(col.name in out)) continue;
    let v = out[col.name] ?? null;
    for (const op of col.normalize) v = applyOp(op, v);
    out[col.name] = v;
  }
  return out;
}
