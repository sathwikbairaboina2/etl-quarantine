import { Validations } from 'aws-cdk-lib';
import type { IConstruct } from 'constructs';
import type { PipelineStack } from './pipeline-stack.js';
import type { StorageStack } from './storage-stack.js';

/*
 * cdk-nag v3 matches acknowledgements by exact finding id, and findings on cross-stack references carry the
 * generated export names. If a construct id changes the hash changes too, the nag test fails and lists the
 * new ids to acknowledge. Every acknowledgement sits on the narrowest construct that owns it and has a reason.
 */
const X = 'EtlQuarantineStorage:ExportsOutputFnGetAtt';
const RAW = `${X}RawBucket0C3EE094ArnD2F95F99/*`;
const STAGING = `${X}StagingBucket9644C37CArnD8583786/*`;
const CURATED = `${X}CuratedBucket6A59C97EArn2BF9884A/*`;
const CURATED_PENDING = `${X}CuratedBucket6A59C97EArn2BF9884A/_pending/*`;
const QUARANTINE = `${X}QuarantineBucketFDBDA180ArnF4FA0A61/*`;

const IAM4_LOGS = 'AwsSolutions-IAM4[Policy::arn:<AWS::Partition>:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole]';
const iam5 = (resource: string) => `AwsSolutions-IAM5[Resource::${resource}]`;

function ack(construct: IConstruct, ids: string[], reason: string): void {
  for (const id of ids) Validations.of(construct).acknowledge({ id, reason });
}

const S3_REASON =
  'S3 object access needs the bucket ARN with /* because object keys are per file and chunk. Each function is limited to the actions and prefixes it uses (validate-transform may write curated only under _pending/*); no action contains a wildcard (asserted in infra/test/pipeline.test.ts).';

export function applySuppressions(storage: StorageStack, pipeline: PipelineStack): void {
  for (const bucket of [storage.raw, storage.staging, storage.curated, storage.quarantine]) {
    ack(
      bucket,
      ['AwsSolutions-S1'],
      'v0.1 is not deployed. Server access logging needs a dedicated log bucket, which is deferred with the rest of the audit trail (README, Known limits).',
    );
  }

  ack(
    storage,
    [IAM4_LOGS],
    'The only role in this stack is the CDK-managed S3 notifications helper, which uses AWSLambdaBasicExecutionRole for its own logs. It runs at deploy time and touches no data.',
  );

  const f = pipeline.functions;
  const logsReason =
    'AWSLambdaBasicExecutionRole is the AWS managed policy for CloudWatch Logs only; every data-plane permission is an explicit, named statement on this role.';
  ack(f.register.role!, [IAM4_LOGS], logsReason);
  ack(f.split.role!, [IAM4_LOGS], logsReason);
  ack(f.validateTransform.role!, [IAM4_LOGS], logsReason);
  ack(f.reconcile.role!, [IAM4_LOGS], logsReason);

  ack(f.register.role!, [iam5(RAW)], S3_REASON);
  ack(f.split.role!, [iam5(RAW), iam5(STAGING)], S3_REASON);
  ack(f.validateTransform.role!, [iam5(STAGING), iam5(CURATED_PENDING), iam5(QUARANTINE)], S3_REASON);
  ack(f.reconcile.role!, [iam5(CURATED), iam5(QUARANTINE)], S3_REASON);

  const fnArn = (fn: { node: { defaultChild?: unknown } }) => pipeline.getLogicalId(fn.node.defaultChild as never);
  const invoke = Object.values(f).map((fn) => iam5(`<${fnArn(fn)}.Arn>:*`));
  ack(
    pipeline.stateMachine,
    [
      ...invoke,
      iam5('*'),
      iam5('arn:<AWS::Partition>:s3:::EtlQuarantineStorage:ExportsOutputRefStagingBucket9644C37C80747B50/*'),
      iam5(
        `arn:<AWS::Partition>:states:<AWS::Region>:<AWS::AccountId>:execution:{"Fn::Select":[6,{"Fn::Split":[":",{"Ref":"${pipeline.getLogicalId(pipeline.stateMachine.node.defaultChild as never)}"}]}]}/*:*`,
      ),
      iam5(
        `arn:<AWS::Partition>:states:<AWS::Region>:<AWS::AccountId>:execution:{"Fn::Select":[6,{"Fn::Split":[":",{"Ref":"${pipeline.getLogicalId(pipeline.stateMachine.node.defaultChild as never)}"}]}]}:*`,
      ),
    ],
    'CDK-generated state machine permissions: invoke the four pipeline functions (and their versions), read the staging manifest for the Distributed Map item reader, start child executions of this state machine, and X-Ray segment writes, which do not support resource-level permissions.',
  );
}
