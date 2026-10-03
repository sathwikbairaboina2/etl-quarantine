import { parquetReadObjects } from 'hyparquet';
import { parquetWriteBuffer } from 'hyparquet-writer';
import { cast, type CastValue } from '../core/cast.js';
import type { ColumnSpec, ValidRow } from '../core/types.js';

type SchemaEl = {
  name: string;
  type?: 'INT32' | 'INT64' | 'BYTE_ARRAY';
  converted_type?: 'UTF8' | 'DATE';
  repetition_type?: 'REQUIRED' | 'OPTIONAL';
  num_children?: number;
};

function schemaFor(columns: ColumnSpec[]): SchemaEl[] {
  const schema: SchemaEl[] = [{ name: 'root', num_children: columns.length }];
  for (const c of columns) {
    const el: SchemaEl = { name: c.outputName ?? c.name, repetition_type: c.required ? 'REQUIRED' : 'OPTIONAL' };
    if (c.type === 'string') {
      el.type = 'BYTE_ARRAY';
      el.converted_type = 'UTF8';
    } else if (c.type === 'int64_cents') {
      el.type = 'INT64';
    } else {
      el.type = 'INT32';
      el.converted_type = 'DATE';
    }
    schema.push(el);
  }
  return schema;
}

/** Only accepts rows that passed validation; casting is therefore infallible for well-formed schemas. */
export function writeParquet(rows: ValidRow[], columns: ColumnSpec[]): Uint8Array {
  const casted = rows.map((r) => cast(r, columns));
  const columnData = columns.map((c) => {
    const name = c.outputName ?? c.name;
    const data: CastValue[] = casted.map((r) => r[name] ?? null);
    return { name, data: data as unknown[] };
  });
  const buf = parquetWriteBuffer({
    columnData: columnData as never,
    schema: schemaFor(columns) as never,
  });
  return new Uint8Array(buf);
}

export async function readParquetRows(bytes: Uint8Array): Promise<Record<string, unknown>[]> {
  const ab = bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer;
  return (await parquetReadObjects({ file: ab })) as Record<string, unknown>[];
}
