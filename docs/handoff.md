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

