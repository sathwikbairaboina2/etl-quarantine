import _Ajv2020 from 'ajv/dist/2020.js';
import _addFormats from 'ajv-formats';
import type { ValidateFunction } from 'ajv/dist/2020.js';
import type { RawRecord, RowError, ValidRow } from './types.js';

const Ajv2020 = _Ajv2020 as unknown as typeof _Ajv2020.default;
const addFormats = _addFormats as unknown as typeof _addFormats.default;

export type Validator = ValidateFunction;

const cache = new Map<string, Validator>();

/** Compiles once per schema $id. */
export function createValidator(schema: Record<string, unknown>): Validator {
  const id = typeof schema.$id === 'string' ? schema.$id : undefined;
  if (id) {
    const hit = cache.get(id);
    if (hit) return hit;
  }
  const ajv = new Ajv2020({ allErrors: true, strict: true, coerceTypes: false });
  addFormats(ajv);
  const v = ajv.compile(schema);
  if (id) cache.set(id, v);
  return v;
}

export type ValidateResult = { ok: true; row: ValidRow } | { ok: false; errors: RowError[] };

export function validateRow(validator: Validator, record: RawRecord): ValidateResult {
  if (validator(record)) return { ok: true, row: record as unknown as ValidRow };
  const errors: RowError[] = (validator.errors ?? []).map((e) => ({
    instancePath: e.instancePath,
    keyword: e.keyword,
    message: e.message ?? '',
  }));
  return { ok: false, errors };
}
