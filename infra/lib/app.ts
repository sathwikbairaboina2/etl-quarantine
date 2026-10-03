import { App, Validations } from 'aws-cdk-lib';
import { AwsSolutionsChecks } from 'cdk-nag';
import { applySuppressions } from './nag-suppressions.js';
import { PipelineStack } from './pipeline-stack.js';
import { StorageStack } from './storage-stack.js';

/** Builds both stacks with the AwsSolutions checks attached. Tests pass a stub `lambdaCodeDir`. */
export function createApp(opts: { lambdaCodeDir?: string; outdir?: string } = {}) {
  const app = new App({ ...(opts.outdir ? { outdir: opts.outdir } : {}) });
  const storage = new StorageStack(app, 'EtlQuarantineStorage');
  const pipeline = new PipelineStack(app, 'EtlQuarantinePipeline', {
    storage,
    ...(opts.lambdaCodeDir ? { lambdaCodeDir: opts.lambdaCodeDir } : {}),
  });
  applySuppressions(storage, pipeline);
  // cdk-nag v3 runs as a policy-validation plugin: unacknowledged violations fail `app.synth()`.
  Validations.of(app).addPlugins(new AwsSolutionsChecks(app, { verbose: true }));
  return { app, storage, pipeline };
}
