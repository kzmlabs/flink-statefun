---
title: "ADR-0007: restart-strategy key names"
description: Dropping the execution. prefix from restart-strategy configuration keys after it silently disabled restart limits on Flink 2.x.
---

# ADR-0007: restart-strategy key names

| | |
|---|---|
| Status | Accepted |
| Date | 2026-07-25 |
| Issues/PRs | PR #271 |

## Context

Flink 2.x defines the restart strategy configuration under `restart-strategy.type` and `restart-strategy.fixed-delay.*`, with no `execution.` prefix. This was verified directly against `RestartStrategyOptions` in `flink-core` 2.2.0: no `execution.restart-strategy.*` keys exist there, not even as deprecated aliases. The repository still used `execution.restart-strategy.*` in the K8s E2E `FlinkDeployment` manifest, in two docs pages, in the quickstart `docker-compose` file, and in the embedded smoke-test harness, a leftover from the Flink 1.x key naming.

Flink does not fail or warn on an unrecognized configuration key; it silently ignores it and falls back to its own default, in this case FLIP-364's default exponential-delay restart strategy with unlimited attempts. This was verified live: with the stale `execution.` prefixed keys in place, a poison-pill record (a null-key Kafka record) kept a job flapping in `RESTARTING` for over 8 minutes with no terminal state. After correcting the keys to `restart-strategy.*`, the same record produced a terminal `FAILED` after exactly the configured 1 initial attempt plus 3 fixed-delay retries, in about 21 seconds.

## Decision

Use `restart-strategy.type` and `restart-strategy.fixed-delay.*` (no `execution.` prefix) everywhere the repository configures a restart strategy: the K8s E2E `FlinkDeployment`, the quickstart `docker-compose` file, the embedded smoke harness, and the docs. Docs call out the `execution.` prefix explicitly as a mistake to avoid, since Flink's silent-ignore behavior gives no other signal that the keys are wrong.

## Consequences

- A poison-pill record now reaches a terminal `FAILED` state after the configured restart attempts, instead of restart-looping indefinitely.
- The general failure mode, that Flink silently ignores unrecognized configuration keys rather than rejecting them, is now documented, since it also affected other renamed Flink 2.x keys (`state.backend.type`, `high-availability.type`).
- Anyone deploying with an old `execution.restart-strategy.*` config carried over from a Flink 1.x setup gets no error signal from Flink itself; the docs are the primary defense against silently getting the unlimited-restart default.
