# Ledger: etl-quarantine v0.1 (2026-10-04)

Plan: `docs/superpowers/plans/2026-10-04-etl-quarantine.md` (25 tasks)
Spec: `docs/superpowers/specs/2026-10-04-etl-quarantine.md`
ADRs: `docs/adr/0001`-`0007`
Commits: authorized, local only. Never push or add remotes. Messages end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
Ports: 5340-5349 only. Containers: `etl-quarantine-*`, removed after use.

Format: one line per task, `Task N: complete (<real test counts / outputs>) | commit: "<subject>"`. Deviations: `Ruling: <what> - <why> - <cost>`. A builder that runs out of context writes `Task N: partial (<what is done, what is next>)` and stops; the next builder resumes from the first task that is not `complete`.

Plan (Opus, 2026-10-04): spec, ADRs 0001-0007 and plan written. Prototypes in scratch (not in repo) confirmed: hyparquet-writer explicit-schema DATE/INT64 round-trip; csv-parse 7 BOM/CRLF/quoted newline; Ajv 2020 cast trick under NodeNext; DynamoDB Local 3.3.1 conditional put + attempt guard; S3Mock 4.11.0 copy/delete/head/404/pagination; CDK 2.272.0 DistributedMap + S3JsonItemReader, $$.State.RetryCount payload, Match.wildcard. | commit: "docs: add v0.1 spec, ADRs, implementation plan and ledger"
Ruling: no LocalStack anywhere (no token) - ADR 0002 - the deployed state machine is proven only by synth assertions
Ruling: v0.1 includes fix/discard/replay/promote and lineage but not Glue/Athena, the LLM suggester, JSONL or schema diff - ADR 0007 - I9 and I10 unimplemented
Ruling: no cost-per-million-rows number is published - nothing is deployed, so it cannot be measured - the design's second headline metric waits for a deploy
Task 1: complete (typecheck exit 0; vitest 1 passed) | commit: "chore: scaffold etl-quarantine package"
Task 2: complete (vitest test/core 10 passed; typecheck exit 0) | commit: "feat(core): add manifest types, customers dataset and purity guard"
Task 3: complete (vitest test/core 40 passed cumulative; typecheck exit 0) | commit: "feat(core): normalise, validate with Ajv 2020-12 and cast valid rows"
Task 4: complete (vitest test/core 64 passed cumulative incl. fast-check 500 runs; typecheck exit 0) | commit: "feat(core): add row accounting, limits, key layout and lineage rules"
Task 5: complete (csv tests included in 64 passed; typecheck exit 0) | commit: "feat(core): map CSV fields to records with column-count errors"
Task 6: complete (vitest test/adapters memory contracts pass; typecheck exit 0) | commit: "feat(adapters): define ports and in-memory stores with a shared contract suite"
Task 7: complete (vitest test/adapters 37 passed total; typecheck exit 0) | commit: "feat(adapters): add filesystem object store and JSON-file control store"
Ruling: added .gitattributes (text=auto eol=lf, csv -text) - Windows autocrlf would alter fixture bytes/shas - none
Task 8: complete (vitest register 8 passed; typecheck exit 0) | commit: "feat(pipeline): register files by content hash with duplicate detection"
Task 9: complete (vitest test/pipeline 18 passed total; typecheck exit 0) | commit: "feat(pipeline): stream-split CSV files into staged chunks"
Ruling: added optional Deps.chunkRows override of manifest.chunkRows - plan tests/bench need small chunks (12 rows/5, 100) without mutating the registry - none, defaults to manifest value
Task 10: complete (parquet 3 passed; typecheck exit 0) | commit: "feat(pipeline): write curated Parquet parts with explicit schema"
Task 11: complete (validate-transform 7 passed; typecheck exit 0) | commit: "feat(pipeline): validate and transform chunks into pending Parquet and quarantine"
Task 12: complete (reconcile 9 passed; test/pipeline 37 passed total; typecheck exit 0) | commit: "feat(pipeline): reconcile row accounting, hold over-threshold files, promote"
Task 13: complete (generate 5 passed; fixtures: clean 200 rows/0 bad, 3pct 1000 rows/26 bad, 20pct 200 rows/40 bad; typecheck exit 0) | commit: "test: add seeded customer fixture generator and golden inputs"
Task 14: complete (driver golden 3 + faults 6 passed; full npm test 22 files 153 tests passed; goldens: clean LOADED, 3pct LOADED_WITH_QUARANTINE 974/26, 20pct HELD 160/40; typecheck exit 0) | commit: "feat(pipeline): add in-process driver with fault injection and file readback"
Task 15: complete (replay 7 passed; full npm test all passed; typecheck exit 0) | commit: "feat(pipeline): fix, discard and replay quarantined rows with lineage"
Ruling: ReplayEdge gained optional rowHashes (sha256 of each replayed row content); replay skips open rows whose content was already sent unchanged - the plan only compared whole-file childSha, which re-replayed a still-invalid row after a partial first replay - extra optional attribute on the REPLAY item, no schema change
Ruling: Task 12 stub applyReplayOutcome moved into lineage-update.ts alongside loadQuarantine/refreshParentStatus (avoids a fix<->lineage import cycle) - none
