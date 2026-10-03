import { describe } from 'vitest';
import { controlStoreContract, objectStoreContract } from '../adapters/contract.js';
import { newAwsEnv } from './support.js';

describe.skipIf(!process.env.ETL_IT)('AWS adapters on DynamoDB Local and S3Mock', () => {
  let env: ReturnType<typeof newAwsEnv> | undefined;
  const lazy = () => (env ??= newAwsEnv());
  objectStoreContract('s3mock', async () => (await lazy()).objects);
  controlStoreContract('dynamodb-local', async () => (await lazy()).control);
});
