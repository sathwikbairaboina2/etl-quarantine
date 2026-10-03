# Handoff

## 2026-10-04, Claude (Sonnet builder), branch main

**What changed.** Built v0.1 from the plan in `docs/superpowers/plans/2026-10-04-etl-quarantine.md` (25 tasks, one commit each; the ledger is `.superpowers/sdd/2026-10-04-etl-quarantine/progress.md`):

- Pure core (`src/core`), ports and four adapters (memory, file system + JSON file, S3, DynamoDB) behind one contract suite.
- Pipeline steps (register, split, validate-transform, reconcile), operator actions (fix, discard, replay, promote) with lineage, an in-process driver with fault injection, and file readback.
- `etl` CLI with a 30-second demo, Lambda handlers with esbuild bundles, CDK storage and pipeline stacks with cdk-nag, an emulator integration suite, CI, a pack smoke test.
- Measured benchmark (`bench/results.json`): 1,000,000 rows, 18 injected chunk crashes, a duplicate delivery: lost 0, duplicated 0, 15,013 rows/s; 20 of 20 sweep runs conserved.
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
