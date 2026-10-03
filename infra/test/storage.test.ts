import { App } from 'aws-cdk-lib';
import { Match, Template } from 'aws-cdk-lib/assertions';
import { describe, expect, it } from 'vitest';
import { StorageStack } from '../lib/storage-stack.js';

const template = () => Template.fromStack(new StorageStack(new App(), 'Storage'));

describe('StorageStack (I11: encryption and retention)', () => {
  it('has four buckets, each encrypted and fully blocked from public access', () => {
    const t = template();
    t.resourceCountIs('AWS::S3::Bucket', 4);
    const buckets = t.findResources('AWS::S3::Bucket');
    for (const [id, b] of Object.entries(buckets)) {
      expect(b.Properties.BucketEncryption.ServerSideEncryptionConfiguration[0].ServerSideEncryptionByDefault.SSEAlgorithm, id).toBe('AES256');
      expect(b.Properties.PublicAccessBlockConfiguration, id).toEqual({
        BlockPublicAcls: true,
        BlockPublicPolicy: true,
        IgnorePublicAcls: true,
        RestrictPublicBuckets: true,
      });
      expect(b.DeletionPolicy, id).toBe('Retain');
    }
  });

  it('every bucket has a policy denying non-TLS requests', () => {
    const t = template();
    const policies = t.findResources('AWS::S3::BucketPolicy');
    expect(Object.keys(policies)).toHaveLength(4);
    for (const p of Object.values(policies)) {
      const stmts = p.Properties.PolicyDocument.Statement as Array<Record<string, any>>;
      expect(
        stmts.some((s) => s.Effect === 'Deny' && s.Condition?.Bool?.['aws:SecureTransport'] === 'false' && s.Principal?.AWS === '*'),
      ).toBe(true);
    }
  });

  it('turns on EventBridge notifications for the raw bucket only', () => {
    const t = template();
    const notif = t.findResources('Custom::S3BucketNotifications');
    const all = Object.values(notif);
    expect(all).toHaveLength(1);
    expect(all[0]!.Properties.NotificationConfiguration).toEqual({ EventBridgeConfiguration: {} });
    const rawId = Object.keys(t.findResources('AWS::S3::Bucket')).find((k) => k.startsWith('RawBucket'))!;
    expect(JSON.stringify(all[0]!.Properties.BucketName)).toContain(rawId);
  });

  it('expires raw after 30 days, staging after 7 and quarantine after 90; curated never', () => {
    const t = template();
    const byPrefix = (p: string) => Object.entries(t.findResources('AWS::S3::Bucket')).find(([k]) => k.startsWith(p))![1];
    const days = (p: string) => byPrefix(p).Properties.LifecycleConfiguration?.Rules?.map((r: any) => r.ExpirationInDays);
    expect(days('RawBucket')).toEqual([30]);
    expect(days('StagingBucket')).toEqual([7]);
    expect(days('QuarantineBucket')).toEqual([90]);
    expect(byPrefix('CuratedBucket').Properties.LifecycleConfiguration).toBeUndefined();
  });

  it('has an on-demand pk/sk control table with point-in-time recovery, retained', () => {
    const t = template();
    t.resourceCountIs('AWS::DynamoDB::GlobalTable', 1);
    t.hasResourceProperties('AWS::DynamoDB::GlobalTable', {
      BillingMode: 'PAY_PER_REQUEST',
      KeySchema: [
        { AttributeName: 'pk', KeyType: 'HASH' },
        { AttributeName: 'sk', KeyType: 'RANGE' },
      ],
      AttributeDefinitions: Match.arrayWith([
        { AttributeName: 'pk', AttributeType: 'S' },
        { AttributeName: 'sk', AttributeType: 'S' },
      ]),
      Replicas: [Match.objectLike({ PointInTimeRecoverySpecification: { PointInTimeRecoveryEnabled: true } })],
    });
    const table = Object.values(t.findResources('AWS::DynamoDB::GlobalTable'))[0]!;
    expect(table.DeletionPolicy).toBe('Retain');
  });
});
