# ADR 0001: One TypeScript package with a pure core behind ports

Status: accepted, 2026-10-04

## Context

The pipeline must run as AWS Lambdas, as a local CLI, inside a benchmark and inside tests. The design doc wants a `core/` with no AWS SDK imports. A monorepo (core, cli, infra packages) is the usual shape, but it adds build ordering, workspace tooling and cross-package type plumbing for a v0.1 that one person maintains.

## Decision

- One npm package, `etl-quarantine`, ESM, TypeScript 5.9.3, Node 24.
- `src/core/` is pure: no `@aws-sdk/*`, no `node:fs`. A test reads the imports and fails if one appears.
- `src/ports.ts` defines `ObjectStore` and `ControlStore`. Adapters live in `src/adapters/`: in-memory, filesystem and JSON-file stores for tests and local runs, S3 and DynamoDB stores for Lambda.
- `src/pipeline/` holds each step as a plain async function over the ports. Lambda handlers in `src/handlers/` and the local driver call the same functions with the same payloads.
- `infra/` (CDK) lives in the same repo. `aws-cdk-lib` is a devDependency, and `infra/` is not shipped in the npm tarball (`files: ["dist", "README.md", "LICENSE"]`). Dataset manifests and schemas live in `src/datasets/` and are imported as JSON, so they ship inside `dist/` and inside each Lambda bundle.

## Consequences

- One `npm ci`, one test runner, one lockfile.
- What we gave up: a separately versioned core library and a Construct Hub package. Publishing the validator and quarantine writer as a reusable CDK construct (a design-doc stretch goal) needs a split later.
- The CLI package carries the AWS SDK clients as dependencies even though local mode does not use them, so the tarball is bigger than a pure local tool needs.
