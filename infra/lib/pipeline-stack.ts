import { resolve } from 'node:path';
import { Duration, RemovalPolicy, Stack, type StackProps } from 'aws-cdk-lib';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import * as tasks from 'aws-cdk-lib/aws-stepfunctions-tasks';
import type { Construct } from 'constructs';
import type { StorageStack } from './storage-stack.js';

export interface PipelineStackProps extends StackProps {
  storage: Pick<StorageStack, 'raw' | 'staging' | 'curated' | 'quarantine' | 'table'>;
  /** Directory holding `<function>/index.mjs` bundles (npm run bundle writes build/lambda). */
  lambdaCodeDir?: string;
}

const TABLE_ACTIONS = ['dynamodb:GetItem', 'dynamodb:PutItem', 'dynamodb:UpdateItem', 'dynamodb:Query'];

export class PipelineStack extends Stack {
  readonly stateMachine: sfn.StateMachine;
  readonly functions: Record<'register' | 'split' | 'validateTransform' | 'reconcile', lambda.Function>;

  constructor(scope: Construct, id: string, props: PipelineStackProps) {
    super(scope, id, props);
    const { raw, staging, curated, quarantine, table } = props.storage;
    const codeDir = props.lambdaCodeDir ?? resolve(import.meta.dirname, '../../build/lambda');

    const environment = {
      RAW_BUCKET: raw.bucketName,
      STAGING_BUCKET: staging.bucketName,
      CURATED_BUCKET: curated.bucketName,
      QUARANTINE_BUCKET: quarantine.bucketName,
      CONTROL_TABLE: table.tableName,
    };

    // Explicit statements only (no grant* helpers), so every action is named and none contains a wildcard.
    const allow = (actions: string[], resources: string[]) => new iam.PolicyStatement({ actions, resources });
    const objects = (b: { bucketArn: string }, path = '*') => `${b.bucketArn}/${path}`;

    const fn = (
      name: string,
      dir: string,
      opts: { timeout: Duration; memory?: number; statements: iam.PolicyStatement[] },
    ) => {
      const f = new lambda.Function(this, `${name}Fn`, {
        runtime: lambda.Runtime.NODEJS_24_X,
        architecture: lambda.Architecture.ARM_64,
        handler: 'index.handler',
        code: lambda.Code.fromAsset(resolve(codeDir, dir)),
        timeout: opts.timeout,
        memorySize: opts.memory ?? 512,
        environment,
        logGroup: new logs.LogGroup(this, `${name}Logs`, { retention: logs.RetentionDays.ONE_MONTH, removalPolicy: RemovalPolicy.DESTROY }),
      });
      for (const s of opts.statements) f.addToRolePolicy(s);
      return f;
    };

    const tableArn = [table.tableArn];
    this.functions = {
      register: fn('Register', 'register', {
        timeout: Duration.minutes(5),
        statements: [allow(['s3:GetObject'], [objects(raw)]), allow(TABLE_ACTIONS, tableArn)],
      }),
      split: fn('Split', 'split', {
        timeout: Duration.minutes(15),
        statements: [
          allow(['s3:GetObject'], [objects(raw)]),
          allow(['s3:PutObject', 's3:DeleteObject'], [objects(staging)]),
          allow(TABLE_ACTIONS, tableArn),
        ],
      }),
      validateTransform: fn('ValidateTransform', 'validate-transform', {
        timeout: Duration.minutes(2),
        memory: 1024,
        statements: [
          allow(['s3:GetObject'], [objects(staging)]),
          allow(['s3:PutObject'], [objects(curated, '_pending/*')]),
          allow(['s3:PutObject'], [objects(quarantine)]),
          allow(TABLE_ACTIONS, tableArn),
        ],
      }),
      reconcile: fn('Reconcile', 'reconcile', {
        timeout: Duration.minutes(5),
        statements: [
          allow(['s3:GetObject', 's3:PutObject', 's3:DeleteObject'], [objects(curated)]),
          allow(['s3:ListBucket'], [curated.bucketArn, quarantine.bucketArn]),
          allow(['s3:GetObject'], [objects(quarantine)]),
          allow([...TABLE_ACTIONS, 'dynamodb:BatchWriteItem'], tableArn),
        ],
      }),
    };

    // Register -> Duplicate? -> Split -> Map(ValidateTransform, retry) -> Reconcile
    const registerTask = new tasks.LambdaInvoke(this, 'Register', {
      lambdaFunction: this.functions.register,
      payload: sfn.TaskInput.fromObject({ key: sfn.JsonPath.stringAt('$.detail.object.key') }),
      payloadResponseOnly: true,
      resultPath: '$.file',
    });
    const splitTask = new tasks.LambdaInvoke(this, 'Split', {
      lambdaFunction: this.functions.split,
      payload: sfn.TaskInput.fromObject({ sha: sfn.JsonPath.stringAt('$.file.sha') }),
      payloadResponseOnly: true,
      resultPath: '$.split',
    });
    const validateTask = new tasks.LambdaInvoke(this, 'ValidateTransform', {
      lambdaFunction: this.functions.validateTransform,
      payload: sfn.TaskInput.fromObject({
        sha: sfn.JsonPath.stringAt('$.sha'),
        chunk: sfn.JsonPath.objectAt('$'),
        attempt: sfn.JsonPath.numberAt('$$.State.RetryCount'),
      }),
      payloadResponseOnly: true,
      retryOnServiceExceptions: false,
    });
    validateTask.addRetry({ errors: ['States.ALL'], maxAttempts: 2, interval: Duration.seconds(2), backoffRate: 2 });

    const reconcilePayload = sfn.TaskInput.fromObject({ sha: sfn.JsonPath.stringAt('$.file.sha') });
    const reconcileTask = new tasks.LambdaInvoke(this, 'Reconcile', {
      lambdaFunction: this.functions.reconcile,
      payload: reconcilePayload,
      payloadResponseOnly: true,
      resultPath: '$.reconcile',
    });
    // When a chunk exhausts its retries the map fails; reconcile then sees the missing chunk result and marks the file FAILED.
    const markFailed = new tasks.LambdaInvoke(this, 'MarkFailed', {
      lambdaFunction: this.functions.reconcile,
      payload: reconcilePayload,
      payloadResponseOnly: true,
      resultPath: '$.reconcile',
    }).next(new sfn.Fail(this, 'ChunkFailed', { error: 'ChunkFailed', cause: 'A chunk failed after its retries' }));

    const chunks = new sfn.DistributedMap(this, 'ProcessChunks', {
      itemReader: new sfn.S3JsonItemReader({ bucket: staging, key: sfn.JsonPath.stringAt('$.split.manifestKey') }),
      maxConcurrency: 20,
      toleratedFailurePercentage: 0,
      resultPath: sfn.JsonPath.DISCARD,
    });
    chunks.itemProcessor(validateTask);
    chunks.addCatch(markFailed, { resultPath: '$.error' });

    const definition = registerTask
      .next(
        new sfn.Choice(this, 'Duplicate or failed?')
          .when(sfn.Condition.stringEquals('$.file.status', 'DUPLICATE'), new sfn.Succeed(this, 'AlreadyLoaded'))
          .when(sfn.Condition.stringEquals('$.file.status', 'FAILED'), new sfn.Fail(this, 'RegisterFailed', { error: 'RegisterFailed', cause: 'File rejected at registration' }))
          .otherwise(
            splitTask.next(
              new sfn.Choice(this, 'Split ok?')
                .when(sfn.Condition.stringEquals('$.split.status', 'FAILED'), new sfn.Fail(this, 'SplitFailed', { error: 'SplitFailed', cause: 'File could not be split' }))
                .otherwise(chunks.next(reconcileTask)),
            ),
          ),
      );

    this.stateMachine = new sfn.StateMachine(this, 'IngestStateMachine', {
      stateMachineType: sfn.StateMachineType.STANDARD,
      definitionBody: sfn.DefinitionBody.fromChainable(definition),
      timeout: Duration.hours(6),
      tracingEnabled: true,
      logs: {
        destination: new logs.LogGroup(this, 'IngestStateMachineLogs', { retention: logs.RetentionDays.ONE_MONTH, removalPolicy: RemovalPolicy.DESTROY }),
        level: sfn.LogLevel.ALL,
      },
    });

    new events.Rule(this, 'RawObjectCreated', {
      description: 'A CSV landed in the raw bucket under dataset=<name>/',
      eventPattern: {
        source: ['aws.s3'],
        detailType: ['Object Created'],
        detail: {
          bucket: { name: [raw.bucketName] },
          object: { key: events.Match.wildcard('dataset=*.csv'), size: events.Match.greaterThan(0) },
        },
      },
      targets: [new targets.SfnStateMachine(this.stateMachine)],
    });
  }
}
