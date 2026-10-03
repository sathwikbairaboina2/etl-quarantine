import { parseManifest } from '../core/manifest.js';
import type { Manifest } from '../core/types.js';
import customersManifest from './customers/manifest.json' with { type: 'json' };
import customersSchema from './customers/v1.schema.json' with { type: 'json' };

export class UnknownDatasetError extends Error {
  override name = 'UnknownDatasetError';
}

export interface Dataset {
  manifest: Manifest;
  schema: Record<string, unknown>;
}

const datasets = new Map<string, Dataset>([
  ['customers', { manifest: parseManifest(customersManifest), schema: customersSchema as Record<string, unknown> }],
]);

export function getDataset(name: string): Dataset {
  const d = datasets.get(name);
  if (!d) throw new UnknownDatasetError(`unknown dataset ${JSON.stringify(name)}`);
  return d;
}

export function listDatasets(): string[] {
  return [...datasets.keys()].sort();
}
