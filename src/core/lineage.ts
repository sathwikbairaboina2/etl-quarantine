export type RowState = 'pending' | 'loaded' | 'requarantined' | 'discarded';

export type LineageSummary = Record<RowState, number>;

/** Rows missing from `states` count as pending, so every quarantined row is in exactly one state. */
export function summarize(quarantinedRows: number[], states: Map<number, RowState>): LineageSummary {
  const out: LineageSummary = { pending: 0, loaded: 0, requarantined: 0, discarded: 0 };
  for (const row of new Set(quarantinedRows)) out[states.get(row) ?? 'pending']++;
  return out;
}

export function isSuperseded(s: LineageSummary): boolean {
  const total = s.pending + s.loaded + s.requarantined + s.discarded;
  return total > 0 && s.pending + s.requarantined === 0;
}
