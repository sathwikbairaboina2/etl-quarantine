import fs from 'node:fs';
import path from 'node:path';

export const FIXTURE_DIR = path.resolve(import.meta.dirname, '../fixtures');

export function fixtureBytes(name: string): Buffer {
  return fs.readFileSync(path.join(FIXTURE_DIR, `${name}.csv`));
}

export function fixtureLabels(name: string): { badRows: number[]; kinds: Record<string, string> } {
  return JSON.parse(fs.readFileSync(path.join(FIXTURE_DIR, `${name}.labels.json`), 'utf8'));
}
