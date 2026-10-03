import { COLUMN_TYPES, NORMALIZE_OPS, type ColumnSpec, type ColumnType, type Manifest, type NormalizeOp } from './types.js';

export class ManifestError extends Error {
  override name = 'ManifestError';
}

function fail(msg: string): never {
  throw new ManifestError(msg);
}

function isObject(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}

function parseColumn(c: unknown, i: number): ColumnSpec {
  if (!isObject(c)) fail(`columns[${i}] must be an object`);
  const { name, type, required, normalize, outputName } = c;
  if (typeof name !== 'string' || name === '') fail(`columns[${i}].name must be a non-empty string`);
  if (typeof type !== 'string' || !COLUMN_TYPES.includes(type as ColumnType)) {
    fail(`column ${name}: unknown column type ${JSON.stringify(type)}`);
  }
  if (typeof required !== 'boolean') fail(`column ${name}: required must be a boolean`);
  if (!Array.isArray(normalize)) fail(`column ${name}: normalize must be an array`);
  for (const op of normalize) {
    if (typeof op !== 'string' || !NORMALIZE_OPS.includes(op as NormalizeOp)) {
      fail(`column ${name}: unknown normalize op ${JSON.stringify(op)}`);
    }
  }
  if (outputName !== undefined && (typeof outputName !== 'string' || outputName === '')) {
    fail(`column ${name}: outputName must be a non-empty string`);
  }
  const spec: ColumnSpec = {
    name,
    type: type as ColumnType,
    required,
    normalize: normalize as NormalizeOp[],
  };
  if (outputName !== undefined) spec.outputName = outputName;
  return spec;
}

export function parseManifest(json: unknown): Manifest {
  if (!isObject(json)) fail('manifest must be an object');
  const { dataset, schema, format, header, columns, partitionBy, quarantineThreshold, chunkRows } = json;
  if (typeof dataset !== 'string' || dataset === '') fail('manifest.dataset is required');
  if (typeof schema !== 'string' || schema === '') fail('manifest.schema is required');
  if (format !== 'csv') fail(`manifest.format must be "csv", got ${JSON.stringify(format)}`);
  if (header !== true) fail('manifest.header must be true');
  if (!Array.isArray(columns) || columns.length === 0) fail('manifest.columns is required');
  if (partitionBy !== 'ingest_date') fail('manifest.partitionBy must be "ingest_date"');
  if (typeof quarantineThreshold !== 'number' || !(quarantineThreshold >= 0 && quarantineThreshold <= 1)) {
    fail(`manifest.quarantineThreshold must be within [0,1], got ${JSON.stringify(quarantineThreshold)}`);
  }
  if (typeof chunkRows !== 'number' || !Number.isInteger(chunkRows) || chunkRows < 1) {
    fail(`manifest.chunkRows must be an integer >= 1, got ${JSON.stringify(chunkRows)}`);
  }
  const cols = columns.map(parseColumn);
  const seen = new Set<string>();
  for (const c of cols) {
    if (seen.has(c.name)) fail(`duplicate column name ${c.name}`);
    seen.add(c.name);
  }
  return { dataset, schema, format, header, columns: cols, partitionBy, quarantineThreshold, chunkRows };
}
