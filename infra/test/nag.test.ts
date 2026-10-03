import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { describe, expect, it } from 'vitest';
import { createApp } from '../lib/app.js';
import { stubLambdaDir } from './support.js';

describe('cdk-nag AwsSolutionsChecks', () => {
  it('synthesizes both stacks with zero unsuppressed violations', () => {
    const outdir = fs.mkdtempSync(path.join(os.tmpdir(), 'etlq-nag-'));
    const { app } = createApp({ lambdaCodeDir: stubLambdaDir(), outdir });
    // A violation that is not acknowledged makes synth throw with the rule ids and the offending paths.
    expect(() => app.synth()).not.toThrow();
    const report = path.join(outdir, 'policy-validation-report.json');
    if (fs.existsSync(report)) {
      const text = fs.readFileSync(report, 'utf8');
      expect(JSON.parse(text).success ?? true).not.toBe(false);
    }
  });
});
