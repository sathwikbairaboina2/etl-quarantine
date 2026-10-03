export interface ChunkCounts {
  index?: number;
  rowsIn: number;
  rowsValid: number;
  rowsQuarantined: number;
}

export type CheckResult = { ok: true } | { ok: false; reason: string };

export function checkChunk(c: ChunkCounts): CheckResult {
  if (c.rowsIn !== c.rowsValid + c.rowsQuarantined) {
    const which = c.index === undefined ? 'chunk' : `chunk ${c.index}`;
    return {
      ok: false,
      reason: `${which}: rowsIn ${c.rowsIn} != rowsValid ${c.rowsValid} + rowsQuarantined ${c.rowsQuarantined}`,
    };
  }
  return { ok: true };
}

export function checkFile(chunks: ChunkCounts[], fileRowsIn: number): CheckResult {
  let inSum = 0;
  let validSum = 0;
  let quarSum = 0;
  for (const c of chunks) {
    const r = checkChunk(c);
    if (!r.ok) return r;
    inSum += c.rowsIn;
    validSum += c.rowsValid;
    quarSum += c.rowsQuarantined;
  }
  if (inSum !== fileRowsIn) {
    return { ok: false, reason: `file: chunk rowsIn sum ${inSum} != file rowsIn ${fileRowsIn}` };
  }
  if (validSum + quarSum !== fileRowsIn) {
    return {
      ok: false,
      reason: `file: rowsValid ${validSum} + rowsQuarantined ${quarSum} != file rowsIn ${fileRowsIn}`,
    };
  }
  return { ok: true };
}
