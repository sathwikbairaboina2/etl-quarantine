import { App } from 'aws-cdk-lib';
import { Match, Template } from 'aws-cdk-lib/assertions';
import { beforeAll, describe, expect, it } from 'vitest';
import { PipelineStack } from '../lib/pipeline-stack.js';
import { StorageStack } from '../lib/storage-stack.js';
import { stubLambdaDir } from './support.js';

let t: Template;
let storageTemplate: Template;

beforeAll(() => {
  const app = new App();
  const storage = new StorageStack(app, 'Storage');
  const pipeline = new PipelineStack(app, 'Pipeline', { storage, lambdaCodeDir: stubLambdaDir() });
  t = Template.fromStack(pipeline);
  storageTemplate = Template.fromStack(storage);
});

/** Flattens a CloudFormation Fn::Join into a string, replacing references with <ref>. */
function flatten(v: unknown): string {
  if (typeof v === 'string') return v;
  if (v && typeof v === 'object' && 'Fn::Join' in v) {
    return ((v as any)['Fn::Join'][1] as unknown[]).map(flatten).join('');
  }
  return '<ref>';
}

function definition(): Record<string, any> {
  const sm = Object.values(t.findResources('AWS::StepFunctions::StateMachine'))[0]!;
  return JSON.parse(flatten(sm.Properties.DefinitionString));
}

describe('state machine', () => {
  it('is a Standard machine with the register, split, distributed map and reconcile flow', () => {
    const sm = Object.values(t.findResources('AWS::StepFunctions::StateMachine'))[0]!;
    expect(sm.Properties.StateMachineType).toBe('STANDARD');
    const d = definition();
    expect(d.StartAt).toBe('Register');
    expect(Object.keys(d.States)).toEqual(
      expect.arrayContaining(['Register', 'Duplicate or failed?', 'Split', 'Split ok?', 'ProcessChunks', 'Reconcile', 'MarkFailed']),
    );
    expect(d.States['Register'].ResultPath).toBe('$.file');
    expect(d.States['ProcessChunks'].Next).toBe('Reconcile');
  });

  it('reads the chunk list from S3 with a distributed map, concurrency 20 and zero tolerated failures', () => {
    const map = definition().States['ProcessChunks'];
    expect(map.Type).toBe('Map');
    expect(map.ItemProcessor.ProcessorConfig.Mode).toBe('DISTRIBUTED');
    expect(map.ItemReader.Resource).toContain('s3:getObject');
    expect(map.ItemReader.ReaderConfig.InputType).toBe('JSON');
    expect(map.ItemReader.Parameters['Key.$']).toBe('$.split.manifestKey');
    expect(map.MaxConcurrency).toBe(20);
    // CDK omits the property when it is 0, which is also the Step Functions default: any failed chunk fails the map.
    expect(map.ToleratedFailurePercentage ?? 0).toBe(0);
    expect(map.ToleratedFailureCount).toBeUndefined();
    expect(map.ResultPath).toBeNull();
    expect(map.Catch[0].Next).toBe('MarkFailed');
  });

  it('passes the retry count as the chunk attempt and retries twice (three attempts)', () => {
    const states = definition().States['ProcessChunks'].ItemProcessor.States;
    const task = states['ValidateTransform'];
    const text = JSON.stringify(task);
    expect(text).toContain('"attempt.$":"$$.State.RetryCount"');
    expect(text).toContain('"sha.$":"$.sha"');
    expect(text).toContain('"chunk.$":"$"');
    expect(task.Retry).toHaveLength(1);
    expect(task.Retry[0]).toMatchObject({ ErrorEquals: ['States.ALL'], MaxAttempts: 2, BackoffRate: 2, IntervalSeconds: 2 });
  });

  it('stops on a duplicate or failed registration and on a failed split', () => {
    const d = definition().States;
    const choice = d['Duplicate or failed?'];
    expect(choice.Choices.map((c: any) => [c.StringEquals, d[c.Next].Type])).toEqual([
      ['DUPLICATE', 'Succeed'],
      ['FAILED', 'Fail'],
    ]);
    expect(d['Split ok?'].Choices[0]).toMatchObject({ StringEquals: 'FAILED', Variable: '$.split.status' });
  });
});

describe('event rule', () => {
  it('matches Object Created for dataset=*.csv in the raw bucket with a non-zero size', () => {
    t.hasResourceProperties('AWS::Events::Rule', {
      EventPattern: {
        source: ['aws.s3'],
        'detail-type': ['Object Created'],
        detail: {
          bucket: { name: [{ 'Fn::ImportValue': Match.stringLikeRegexp('RawBucket') }] },
          object: { key: [{ wildcard: 'dataset=*.csv' }], size: [{ numeric: ['>', 0] }] },
        },
      },
    });
    t.resourceCountIs('AWS::Events::Rule', 1);
  });
});

describe('lambda functions and IAM', () => {
  const policiesFor = (fnPrefix: string) => {
    const policies = t.findResources('AWS::IAM::Policy');
    const key = Object.keys(policies).find((k) => k.startsWith(fnPrefix) && k.includes('ServiceRoleDefaultPolicy'));
    expect(key, `policy for ${fnPrefix}`).toBeDefined();
    return (policies[key!]!.Properties.PolicyDocument.Statement as Array<{ Action: string | string[]; Resource: unknown }>).map((s) => ({
      actions: ([] as string[]).concat(s.Action),
      resources: ([] as unknown[]).concat(s.Resource),
    }));
  };
  const fnPrefixes = ['RegisterFn', 'SplitFn', 'ValidateTransformFn', 'ReconcileFn'];

  it('has four nodejs24.x arm64 functions with the planned timeouts and memory', () => {
    t.resourceCountIs('AWS::Lambda::Function', 4);
    const fns = Object.values(t.findResources('AWS::Lambda::Function')).map((f) => f.Properties);
    for (const f of fns) {
      expect(f.Runtime).toBe('nodejs24.x');
      expect(f.Architectures).toEqual(['arm64']);
      expect(f.Handler).toBe('index.handler');
      expect(Object.keys(f.Environment.Variables).sort()).toEqual(['CONTROL_TABLE', 'CURATED_BUCKET', 'QUARANTINE_BUCKET', 'RAW_BUCKET', 'STAGING_BUCKET']);
    }
    const byTimeout = fns.map((f) => [f.Timeout, f.MemorySize]).sort();
    expect(byTimeout).toEqual([
      [120, 1024],
      [300, 512],
      [300, 512],
      [900, 512],
    ]);
  });

  it('keeps one month of logs for every function', () => {
    const groups = Object.values(t.findResources('AWS::Logs::LogGroup')).map((g) => g.Properties.RetentionInDays);
    expect(groups.filter((d) => d === 30).length).toBeGreaterThanOrEqual(4);
  });

  it.each(fnPrefixes)('%s has no wildcard action', (prefix) => {
    for (const s of policiesFor(prefix)) {
      for (const a of s.actions) expect(a, `${prefix}: ${a}`).not.toContain('*');
    }
  });

  it('validate-transform cannot write to the raw bucket and writes curated only under _pending/', () => {
    const stmts = policiesFor('ValidateTransformFn');
    const puts = stmts.filter((s) => s.actions.includes('s3:PutObject'));
    expect(puts).toHaveLength(2);
    const text = JSON.stringify(puts);
    expect(text).not.toContain('RawBucket');
    const curated = puts.find((s) => JSON.stringify(s.resources).includes('CuratedBucket'))!;
    expect(curated.resources).toHaveLength(1);
    expect(JSON.stringify(curated.resources[0])).toMatch(/\/_pending\/\*"/);
    const everything = stmts.flatMap((s) => s.actions);
    expect(everything).not.toContain('s3:DeleteObject');
    expect(everything).not.toContain('dynamodb:BatchWriteItem');
  });

  it('only reconcile can delete curated objects and batch-write the control table', () => {
    for (const p of ['RegisterFn', 'SplitFn', 'ValidateTransformFn']) {
      const actions = policiesFor(p).flatMap((s) => s.actions);
      expect(actions, p).not.toContain('dynamodb:BatchWriteItem');
      const deletes = policiesFor(p).filter((s) => s.actions.includes('s3:DeleteObject'));
      for (const d of deletes) expect(JSON.stringify(d.resources), p).not.toContain('CuratedBucket');
    }
    expect(policiesFor('ReconcileFn').flatMap((s) => s.actions)).toContain('dynamodb:BatchWriteItem');
  });

  it('register can only read raw objects', () => {
    const s3 = policiesFor('RegisterFn').filter((s) => s.actions.some((a) => a.startsWith('s3:')));
    expect(s3.flatMap((s) => s.actions)).toEqual(['s3:GetObject']);
    expect(JSON.stringify(s3[0]!.resources)).toContain('RawBucket');
  });

  it('shares the storage stack through exports, not duplicates', () => {
    expect(Object.keys(storageTemplate.toJSON().Outputs ?? {}).length).toBeGreaterThan(0);
  });
});
