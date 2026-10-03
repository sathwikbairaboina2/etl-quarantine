// Runs the integration suite with ETL_IT=1. Works the same on Windows and Linux (no inline VAR=x).
import { spawn } from 'node:child_process';

const command = ['npx', 'vitest', 'run', 'test/integration', ...process.argv.slice(2)].join(' ');
const child = spawn(command, {
  stdio: 'inherit',
  shell: true,
  env: { ...process.env, ETL_IT: '1' },
});
child.on('exit', (code) => process.exit(code ?? 1));
