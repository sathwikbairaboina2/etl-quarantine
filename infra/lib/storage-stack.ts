import { Duration, RemovalPolicy, Stack, type StackProps } from 'aws-cdk-lib';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as s3 from 'aws-cdk-lib/aws-s3';
import type { Construct } from 'constructs';

/** Four encrypted buckets and the control table. Nothing here is public and nothing is deleted with the stack. */
export class StorageStack extends Stack {
  readonly raw: s3.Bucket;
  readonly staging: s3.Bucket;
  readonly curated: s3.Bucket;
  readonly quarantine: s3.Bucket;
  readonly table: dynamodb.TableV2;

  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const bucket = (name: string, extra: Partial<s3.BucketProps> = {}) =>
      new s3.Bucket(this, name, {
        blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
        encryption: s3.BucketEncryption.S3_MANAGED,
        enforceSSL: true,
        removalPolicy: RemovalPolicy.RETAIN,
        ...extra,
      });

    this.raw = bucket('RawBucket', {
      eventBridgeEnabled: true,
      lifecycleRules: [{ id: 'expire-raw-30d', expiration: Duration.days(30) }],
    });
    this.staging = bucket('StagingBucket', {
      lifecycleRules: [{ id: 'expire-staging-7d', expiration: Duration.days(7) }],
    });
    this.quarantine = bucket('QuarantineBucket', {
      lifecycleRules: [{ id: 'expire-quarantine-90d', expiration: Duration.days(90) }],
    });
    this.curated = bucket('CuratedBucket');

    this.table = new dynamodb.TableV2(this, 'ControlTable', {
      partitionKey: { name: 'pk', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'sk', type: dynamodb.AttributeType.STRING },
      billing: dynamodb.Billing.onDemand(),
      pointInTimeRecoverySpecification: { pointInTimeRecoveryEnabled: true },
      removalPolicy: RemovalPolicy.RETAIN,
    });
  }
}
