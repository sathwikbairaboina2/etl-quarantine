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
Task 16: complete (cli 4 passed; npm run build exit 0; node dist/cli/main.js --version -> 0.1.0; unknown sha -> "error: no file with sha zzz", exit 1; typecheck exit 0) | commit: "feat(cli): add etl command for ingest, status, quarantine, replay and promote"
Task 17: complete (demo 2 tests passed; real run `npm run demo`: 50000 rows, 1557 bad, chunk 2 crash then ok, DUPLICATE on second delivery, 207 fixed + replayed LOADED, 203 discarded; accounting: source 50000, curated 48650, still quarantined 1147, discarded 203, lost 0, duplicated 0, wall 4.9 s) | commit: "feat(cli): add 30-second demo of ingest, crash, duplicate, fix and replay"
Task 18: complete (aws-fakes 19 passed; typecheck exit 0) | commit: "feat(adapters): add S3 and DynamoDB adapters"
Ruling: DynamoControlStore stores a replay edge as REPLAY#<childSha>#<part> items of at most 2000 rows each (merged on read) and listFiles uses the DATASET# index plus BatchGet - plan has one REPLAY#<childSha> item, which would exceed the 400 KB item limit for large replays - sk shape differs from the design doc, behaviour identical
Incident: a shared /tmp/c.sh was overwritten by another session, so my Task 18 commit command committed that session infra-agent working tree (runner.py, test_runner.py) as 76b4df5 in the infra-agent repo with my subject line. I did not rewrite it (they have committed on top). The etl-quarantine Task 18 commit was redone with a repo-pinned script in my scratchpad.
Task 19: complete (1: `npm test` no ETL_IT -> 26 files passed, 2 skipped; 185 tests passed, 20 skipped, exit 0. 2: `npm run it:up && npm run test:it && npm run it:down` -> 2 files, 20 tests passed on DynamoDB Local 3.3.1 + S3Mock 4.11.0. 3: `docker ps -a --filter name=etl-quarantine` empty afterwards) | commit: "test(integration): run adapter contracts and pipeline on DynamoDB Local and S3Mock"
Ruling: vitest hookTimeout 60000 - emulator warm-up exceeded the default 10 s beforeAll - none
Task 20: complete (handlers 8 passed; npm run bundle -> register 1671 KiB, split 1735 KiB, validate-transform 2092 KiB, reconcile 1676 KiB; node import of build/lambda/register/index.mjs handler -> function; typecheck exit 0) | commit: "feat(handlers): add Lambda handlers and esbuild bundles"
Task 21: complete (infra/test/storage 5 passed: 4 buckets AES256+BlockPublicAccess+Retain, TLS-deny policies, EventBridge custom resource on raw only, lifecycle 30/7/90 and none on curated, TableV2 pk/sk on-demand PITR retained; typecheck exit 0) | commit: "feat(infra): add storage stack with encrypted buckets, lifecycle and control table"
Task 22: complete (infra tests 21 passed: storage 5, pipeline 15, nag 1 with zero unacknowledged AwsSolutions violations; npm run synth exit 0 -> cdk.out/EtlQuarantineStorage.template.json 14 resources, cdk.out/EtlQuarantinePipeline.template.json 25 resources (4 Lambda, 1 state machine, 1 rule, 5 log groups, 6 roles, 7 policies); typecheck exit 0) | commit: "feat(infra): add pipeline stack with EventBridge, Distributed Map state machine and least-privilege IAM"
Ruling: cdk-nag 3.0.2 is a policy-validation plugin (Validations.of(app).addPlugins(new AwsSolutionsChecks(app))) and acknowledgements are Validations.of(construct).acknowledge with exact finding ids - the plan assumed v2 Aspects/NagSuppressions, which v3 removed - acknowledged ids embed generated export hashes, so renaming a storage construct fails the nag test until updated
Ruling: split Lambda also gets s3:DeleteObject on staging; state machine log level ALL; DistributedMap Catch -> MarkFailed (reconcile) -> Fail - split.fail() deletes staged chunks, ALL clears AwsSolutions-SF1, and without the Catch a chunk that exhausts retries would leave the file stuck in PROCESSING instead of FAILED - ToleratedFailurePercentage 0 is the CDK/Step Functions default so it is not rendered
Ruling: cdk.json sets @aws-cdk/core:defaultCrossStackReferences=strong - silences the CDK warning, same as the default behaviour
