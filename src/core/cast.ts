import { InvariantError, type ColumnSpec, type ValidRow } from './types.js';

export type CastValue = string | bigint | Date | null;

const CENTS = /^(-?)(\d+)(?:\.(\d{1,2}))?$/;
const DATE = /^(\d{4})-(\d{2})-(\d{2})$/;

/** Exact decimal-to-cents conversion with no floats. */
export function parseCents(s: string): bigint {
  const m = CENTS.exec(s);
  if (!m) throw new InvariantError(`cannot cast ${JSON.stringify(s)} to cents`);
  const [, sign, whole, frac] = m;
  const cents = BigInt(whole!) * 100n + BigInt((frac ?? '').padEnd(2, '0') || '0');
  return sign === '-' ? -cents : cents;
}

export function parseDate(s: string): Date {
  const m = DATE.exec(s);
  if (!m) throw new InvariantError(`cannot cast ${JSON.stringify(s)} to a date`);
  const [y, mo, d] = [Number(m[1]), Number(m[2]), Number(m[3])];
  const date = new Date(Date.UTC(y, mo - 1, d));
  if (date.getUTCFullYear() !== y || date.getUTCMonth() !== mo - 1 || date.getUTCDate() !== d) {
    throw new InvariantError(`invalid calendar date ${JSON.stringify(s)}`);
  }
  return date;
}

export function cast(row: ValidRow, columns: ColumnSpec[]): Record<string, CastValue> {
  const out: Record<string, CastValue> = {};
  for (const col of columns) {
    const v = row[col.name];
    const key = col.outputName ?? col.name;
    if (v === null || v === undefined) {
      if (col.required) throw new InvariantError(`required column ${col.name} is null`);
      out[key] = null;
      continue;
    }
    switch (col.type) {
      case 'string':
        out[key] = v;
        break;
      case 'int64_cents':
        out[key] = parseCents(v);
        break;
      case 'date':
        out[key] = parseDate(v);
        break;
      default:
        throw new InvariantError(`unknown column type for ${col.name}`);
    }
  }
  return out;
}
