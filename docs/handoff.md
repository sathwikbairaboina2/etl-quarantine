# Handoff

## 2026-10-04, Claude (Sonnet builder), branch main

**What changed.** Built v0.1 from the plan in `docs/superpowers/plans/2026-10-04-etl-quarantine.md` (25 tasks, one commit each; the ledger is `.superpowers/sdd/2026-10-04-etl-quarantine/progress.md`):

- Pure core (`src/core`), ports and four adapters (memory, file system + JSON file, S3, DynamoDB) behind one contract suite.
- Pipeline steps (register, split, validate-transform, reconcile), operator actions (fix, discard, replay, promote) with lineage, an in-process driver with fault injection, and file readback.
- `etl` CLI with a 30-second demo, Lambda handlers with esbuild bundles, CDK storage and pipeline stacks with cdk-nag, an emulator integration suite, CI, a pack smoke test.
- Measured benchmark (`bench/results.json`): 1,000,000 rows, 18 injected chunk crashes, a duplicate delivery: lost 0, duplicated 0, 35,950 rows/s (re-run after the review fixes; earlier runs gave 15,013 and 23,184); 20 of 20 sweep runs conserved.
- README with the measured headline and the real demo transcript. `docs/DEVDOCS.md` is not written yet.

**What is left.**

- `docs/DEVDOCS.md` (the Opus session writes it) and the board update.
- Deferred by design (ADR 0007): Glue/Athena, the LLM fix suggester, JSONL input and the schema-diff gate, alerts, a real deploy and any cost number.
- Rulings that differ from the plan are in the ledger (cdk-nag 3 uses `Validations`, replay-edge row hashes, DynamoDB edge parts, `Deps.chunkRows`, `.gitattributes`).

**How to verify.**

```sh
npm ci
npm run gates                                          # typecheck, test, build, synth, smoke:pack
npm run it:up && npm run test:it && npm run it:down    # needs Docker; ports 5340/5341
docker ps --filter name=etl-quarantine                 # must be empty afterwards
npm run bench                                          # about 2 minutes; rewrites bench/results.json
```

---

**2026-10-04, Claude (sonnet-builder), branch main: review fixes.**

- Replay, fix and discard now refuse a parent that is not LOADED_WITH_QUARANTINE or SUPERSEDED (a HELD parent must be promoted first). Discard refuses rows already loaded or discarded.
- A replay child that ended FAILED no longer blocks its rows: replay counts only edges whose child got through. `register` re-admits a FAILED file (new `ControlStore.resetChunks`), so a transient failure can be ingested again. Replay keys use the highest existing generation + 1.
- Integration readiness wait is 120 s (`ETL_IT_WAIT_MS`); cold emulators need about 30 s. The bench headline now also exits 1 unless the status, the duplicate status, retries == injected crashes and 0 pending Parquet rows hold. The demo accounting is per file (`accountRows`).
- Known issue outside this repo: commit 76b4df5 "feat(adapters): add S3 and DynamoDB adapters" in the sibling `infra-agent` repo contains infra-agent code (`src/infra_agent/runner.py`, `tests/test_runner.py`) under an etl-quarantine subject, because a shared `/tmp/c.sh` was overwritten by another session. It was not rewritten from here; its owner can reword it with an interactive rebase. Builders should keep helper scripts in a repo-specific scratch subdirectory (for example `scratchpad/etlq-*`).
- Re-admitting a FAILED file uses an unconditional update, so two simultaneous deliveries of the same FAILED file could both run it; the chunk-attempt rule keeps the output correct but the work is duplicated.


---

## 2026-10-04, Claude (Opus lead), branch main: verification, DEVDOCS, finish

**What changed.**

- Confirmed the five review findings are fixed, each with a regression test: replay, fix and discard refuse FAILED and HELD parents (0 visible rows); discard refuses rows a replay already loaded; a FAILED replay child no longer blocks its rows; the integration readiness wait is 120 s; the infra-agent cross-repo commit is recorded above.
- Closed the leftover from the previous entry: re-admitting a FAILED file is now a conditional `FAILED -> REGISTERED` claim (`updateFile(sha, patch, { ifStatus })`, StateError on mismatch, DynamoDB condition on `status`). A concurrent second delivery is recorded as a DUPLICATE. A contract test covers memory, file and DynamoDB stores, and a register test with two concurrent deliveries fails without the guard.
- `fix` now also refuses rows that are already loaded or discarded (before, it accepted them silently).
- Re-ran the 1M-row benchmark on the final code: 0 lost, 0 duplicated, 20 of 20 sweep runs conserved, 9,667 rows/s with the CPU at about 85% from other sessions. The README reports throughput as the measured range of five runs (9,667 to 44,768 rows/s), not one number.
- Wrote `docs/DEVDOCS.md` (developer guide).
- Found `bench/RESULTS.md` and `bench/results.json` changed but uncommitted from the previous step (a 44,768 rows/s run at 1a93311). I copied them to `%TEMP%/etlq-uncommitted-bench/`; my own benchmark run then replaced them.

**What is left.**

- Deferred by design (ADR 0007): Glue/Athena, the LLM fix suggester, JSONL input, the schema-diff gate, alerts, a real deploy and any cost number.
- CI service containers still have no health checks; the 120 s readiness wait covers it.
- Outside this repo: commit 76b4df5 in `infra-agent` needs its owner to reword it (see above). Not touched from here.

**How to verify.**

```sh
npm ci
npm run gates                                          # 30 files, 225 tests pass, 22 skipped; synth and pack smoke OK
npm run it:up && npm run test:it && npm run it:down    # 2 files, 22 tests; 85 s from cold containers
docker ps -a --filter name=etl-quarantine              # empty afterwards
npm run bench                                          # about 3.5 minutes on a busy machine; lost 0, duplicated 0
```
