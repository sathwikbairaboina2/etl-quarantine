export type ColumnType = 'string' | 'int64_cents' | 'date';
export type NormalizeOp = 'trim' | 'lowercase' | 'uppercase' | 'emptyToNull';

export const COLUMN_TYPES: readonly ColumnType[] = ['string', 'int64_cents', 'date'];
export const NORMALIZE_OPS: readonly NormalizeOp[] = ['trim', 'lowercase', 'uppercase', 'emptyToNull'];

export interface ColumnSpec {
  name: string;
  type: ColumnType;
  required: boolean;
  normalize: NormalizeOp[];
  outputName?: string;
}

export interface Manifest {
  dataset: string;
  schema: string;
  format: 'csv';
  header: true;
  columns: ColumnSpec[];
  partitionBy: 'ingest_date';
  quarantineThreshold: number;
  chunkRows: number;
}

export interface RowError {
  instancePath: string;
  keyword: string;
  message: string;
}

export type RawRecord = Record<string, string | null>;

export interface QuarantineRecord {
  fileSha: string;
  chunk: number;
  rowNumber: number;
  raw: string;
  parsed: RawRecord;
  schemaVersion: string;
  errors: RowError[];
  quarantinedAt: string;
}

declare const validBrand: unique symbol;
export type ValidRow = Readonly<RawRecord> & { readonly [validBrand]: true };

export interface Limits {
  maxFileBytes: number;
  maxRowBytes: number;
  maxColumns: number;
}

export class InvariantError extends Error {
  override name = 'InvariantError';
}
