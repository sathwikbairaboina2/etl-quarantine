import fs from 'node:fs';
import path from 'node:path';
import { generateCustomers, labelsToJson } from '../src/fixtures/generate.js';

function arg(name: string, def?: string): string | undefined {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 ? process.argv[i + 1] : def;
}

const rows = Number(arg('rows', '1000'));
const badRate = Number(arg('bad-rate', '0.03'));
const seed = Number(arg('seed', '1'));
const out = arg('out');
const labelsOut = arg('labels');
if (!out) {
  console.error('usage: gen-fixture --rows N --bad-rate R --seed S --out path [--labels path]');
  process.exit(2);
}

fs.mkdirSync(path.dirname(path.resolve(out)), { recursive: true });
const fd = fs.openSync(out, 'w');
let buf: string[] = [];
let size = 0;
const flush = () => {
  if (buf.length) fs.writeSync(fd, buf.join(''));
  buf = [];
  size = 0;
};
const { labels } = generateCustomers({
  rows,
  badRate,
  seed,
  onLine: (line) => {
    buf.push(line, '\n');
    size += line.length + 1;
    if (size > 1 << 20) flush();
  },
});
flush();
fs.closeSync(fd);
if (labelsOut) fs.writeFileSync(labelsOut, `${JSON.stringify(labelsToJson(labels), null, 2)}\n`);
console.log(`wrote ${rows} rows (${labels.size} bad) to ${out}`);
