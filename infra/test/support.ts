import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

/** A directory with a stub index.mjs per function, so stack tests do not need `npm run bundle`. */
export function stubLambdaDir(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'etlq-lambda-'));
  for (const name of ['register', 'split', 'validate-transform', 'reconcile']) {
    fs.mkdirSync(path.join(dir, name), { recursive: true });
    fs.writeFileSync(path.join(dir, name, 'index.mjs'), 'export const handler = async () => ({});\n');
  }
  return dir;
}
