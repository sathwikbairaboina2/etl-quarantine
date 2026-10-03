import type { Manifest, RawRecord, RowError } from './types.js';

export class HeaderError extends Error {
  override name = 'HeaderError';
}

/** Returns the file's column order. Matching is exact and case-sensitive after trimming and BOM removal. */
export function checkHeader(headerFields: string[], manifest: Manifest): string[] {
  const header = headerFields.map((f, i) => {
    const t = (i === 0 ? f.replace(/^﻿/, '') : f).trim();
    return t;
  });
  const expected = manifest.columns.map((c) => c.name);
  const missing = expected.filter((c) => !header.includes(c));
  const seen = new Set<string>();
  const unexpected: string[] = [];
  for (const h of header) {
    if (!expected.includes(h) || seen.has(h)) unexpected.push(h);
    seen.add(h);
  }
  if (missing.length > 0 || unexpected.length > 0) {
    const parts: string[] = [];
    if (missing.length) parts.push(`missing columns: ${missing.join(', ')}`);
    if (unexpected.length) parts.push(`unexpected columns: ${unexpected.map((u) => JSON.stringify(u)).join(', ')}`);
    throw new HeaderError(`header mismatch for dataset ${manifest.dataset}: ${parts.join('; ')}`);
  }
  return header;
}

export function fieldsToRecord(fields: string[], header: string[]): { record: RawRecord; errors: RowError[] } {
  const record: RawRecord = {};
  header.forEach((name, i) => {
    record[name] = fields[i] ?? null;
  });
  const errors: RowError[] = [];
  if (fields.length !== header.length) {
    errors.push({
      instancePath: '',
      keyword: 'columnCount',
      message: `expected ${header.length} fields, got ${fields.length}`,
    });
  }
  return { record, errors };
}
