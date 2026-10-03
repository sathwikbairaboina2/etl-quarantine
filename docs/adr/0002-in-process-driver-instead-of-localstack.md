# ADR 0002: An in-process driver mirrors the state machine; no LocalStack in v0.1

Status: accepted, 2026-10-04

## Context

The design doc makes LocalStack the deploy and integration target. Since 2026-03-23 LocalStack needs an auth token to start, even on the free Hobby plan, and CDK asset deployment needs a paid plan. No token exists on this machine or in CI. Step Functions Distributed Map and S3-to-EventBridge delivery therefore cannot run locally.

## Decision

- `src/pipeline/driver.ts` runs the same steps in the same order as the `IngestFile` state machine: Register, a duplicate check, Split, a map over chunks with up to 3 attempts per chunk, then Reconcile. It passes the same JSON payloads the Lambda handlers receive.
- The driver takes a `FaultPlan`, so tests, the demo and the benchmark can crash a chunk after it wrote output, or deliver a file twice.
- The real state machine is built in CDK (`infra/lib/pipeline-stack.ts`) and checked by assertion tests: the Distributed Map, its S3 item reader, `MaxConcurrency`, the retry policy, the duplicate `Choice`, and the EventBridge pattern.
- Nothing deploys anywhere in v0.1.

## Consequences

- Tests, the demo and the benchmark run offline in seconds, with no account and no token.
- What we gave up: proof that the deployed state machine behaves like the driver. Untested: EventBridge at-least-once delivery timing, Distributed Map batching and result writing, Lambda timeouts and memory, and IAM at runtime. The README says so.
- If a LocalStack token or an AWS sandbox becomes available, the same handlers deploy unchanged, and the integration suite in ADR 0006 can gain a deployed variant.
