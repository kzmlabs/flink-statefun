# invalidRecordHandling skip/fail Implementation Plan (ADR-0008 stage 2)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the `invalidRecordHandling` policy on the routable Kafka ingress with `skip` (new default: drop + rate-limited ERROR log + metrics, job stays RUNNING) and `fail` (explicit opt-in to the current job-fatal contract). `forward` is a separate later PR.

**Architecture:** The deserializer classifies defects (NULL_KEY, NULL_VALUE) and consults a per-topic policy resolved in `RoutableKafkaIngressSpec` exactly like `forwardHeaders` (ingress-level default + per-topic override). On `fail` it throws the stage-1 pinned `IllegalStateException` (now as a typed subclass); on `skip` it rate-limited-logs and returns null, and `KafkaDeserializationSchemaDelegate` treats a null from the deserializer as "skip this record" (which also fixes the broken `KafkaIngressDeserializer` javadoc contract "return null if the message cannot be deserialized") and increments `numInvalidRecordsSkipped` + FLIP-33 `numRecordsInErrors` counters registered in `open()`.

**Tech Stack:** Jackson spec parsing (flink-shaded), Flink `KafkaRecordDeserializationSchema`, SLF4J, JUnit 5 + AssertJ.

## Global Constraints

- English only in all repo artifacts.
- Java style: no line wrapping to ~100 chars, no `final` on local variables, short HTML-free Javadoc instead of `//` comments.
- No AI attribution in commits.
- Branch: `feature/invalid-record-handling-skip-fail` off `release` AFTER PR #291 merges (E2E scenarios and stage-1 diagnostics must be present).
- Pinned exception contract: the `fail` path must produce byte-identical messages to stage 1 (`The io.statefun.kafka.v1/ingress ingress requires a UTF-8 key set for each record. Offending record: topic [...], partition [...], offset [...], timestamp [...].` and the tombstone equivalent with `, key [...]`).
- Breaking change: default flips from crash to skip — release notes entry is part of this PR.
- TDD: every behavior lands test-first in `RoutableKafkaIngressDeserializerTest`, a new `RoutableKafkaIngressBinderV1Test` fixture, delegate tests, and the E2E class.

---

### Task 1: Policy model + spec parsing

**Files:**
- Create: `statefun-flink/statefun-flink-io-bundle/src/main/java/org/apache/flink/statefun/flink/io/kafka/binders/ingress/v1/InvalidRecordPolicy.java`
- Modify: `statefun-flink/statefun-flink-io-bundle/src/main/java/org/apache/flink/statefun/flink/io/kafka/binders/ingress/v1/RoutableKafkaIngressSpec.java`
- Test: `statefun-flink/statefun-flink-io-bundle/src/test/java/org/apache/flink/statefun/flink/io/kafka/binders/ingress/v1/RoutableKafkaIngressBinderV1Test.java` + new YAML fixture `routable-kafka-ingress-v1-invalid-handling.yaml` next to the existing fixtures

**Interfaces:**
- Produces: `InvalidRecordPolicy` value class (see YAML note above: action SKIP|FAIL + logLevel WARN|ERROR, default `skip` with `warn`); `RoutableKafkaIngressSpec` gains `invalidRecordPolicyByTopic()` returning `Map<String, InvalidRecordPolicy>` resolved per topic (override wins wholesale, else ingress default, else `skip`/`warn`).
- Spec YAML shape (camelCase, `type` discriminator, mirroring `deliverySemantic`):

```yaml
kind: io.statefun.kafka.v1/ingress
spec:
  invalidRecordHandling:
    type: skip            # or fail; omitted entirely -> skip
    logLevel: warn        # skip only: warn (default) or error
  topics:
    - topic: strict.topic
      invalidRecordHandling:
        type: fail        # per-topic override, replaces the ingress-level object wholesale
```

- Policy model is therefore not a bare enum: `InvalidRecordPolicy` is a small value class `{ Action action (SKIP|FAIL), Level logLevel (WARN|ERROR, only meaningful for SKIP, default WARN) }` with static factories `InvalidRecordPolicy.skip(Level)` / `InvalidRecordPolicy.fail()`; `fromSpecNode(ObjectNode)` validates `type` in `[skip, fail]` and `logLevel` in `[warn, error]`, rejecting `logLevel` under `type: fail`.

- [ ] **Step 1: Write failing binder test** — extend `RoutableKafkaIngressBinderV1Test` with a fixture where the ingress sets `invalidRecordHandling: {type: fail}` and one topic overrides back to `skip`; assert `invalidRecordPolicyByTopic()` resolves FAIL for the plain topic and SKIP for the overridden one, and that an unknown `type: explode` fails with a message listing `[skip, fail]`.
- [ ] **Step 2: Run, verify it fails** (no such field/method yet).
- [ ] **Step 3: Implement** — `InvalidRecordPolicy` enum; in the spec: `Optional<InvalidRecordPolicy> invalidRecordHandlingDefault` on the Builder (`@JsonProperty("invalidRecordHandling")` with a small `InvalidRecordHandlingJsonDeserializer` reading the `type` field, same pattern as `StartupPositionJsonDeserializer`); `TopicRouting` gains `InvalidRecordPolicy overrideOrNull` parsed in `TopicRoutingsJsonDeserializer` (same pattern as `parseForwardHeadersOverride`); `invalidRecordPolicyByTopic()` resolves override → ingress default → SKIP.
- [ ] **Step 4: Run tests green.**
- [ ] **Step 5: Commit** `feat(kafka): parse invalidRecordHandling policy with per-topic override`

---

### Task 2: Typed exception + policy-aware deserializer

**Files:**
- Create: `statefun-flink/statefun-flink-io-bundle/src/main/java/org/apache/flink/statefun/flink/io/kafka/binders/ingress/v1/InvalidRecordException.java`
- Modify: `RoutableKafkaIngressDeserializer.java` (same package)
- Test: `RoutableKafkaIngressDeserializerTest.java`

**Interfaces:**
- Produces: `class InvalidRecordException extends IllegalStateException` carrying `enum Defect { NULL_KEY, NULL_VALUE }`, topic, partition, offset (message stays byte-identical to stage 1, so existing greps and the merged E2E fragments keep matching).
- Deserializer constructor gains `Map<String, InvalidRecordPolicy> policyByTopic` (threaded from the spec in `toUniversalKafkaIngressSpec`, like `routingConfigsByTopic()`); on a defect with SKIP policy `deserialize` returns null after logging through the rate limiter (Task 3); with FAIL it throws `InvalidRecordException`.
- Consumes: `InvalidRecordPolicy` from Task 1.

- [ ] **Step 1: Write failing tests** — SKIP policy: null-key record and tombstone both return null (no throw) and the happy path still routes; FAIL policy: both throw `InvalidRecordException` with the exact stage-1 message (reuse the existing coordinate assertions); unknown-topic behavior unchanged.
- [ ] **Step 2: Run, verify wrong-reason failures are absent** (constructor signature change breaks compilation first — fix test helpers, re-run until the assertions themselves fail).
- [ ] **Step 3: Implement minimal policy branch in the deserializer.**
- [ ] **Step 4: Green + whole-module `mvn -pl statefun-flink/statefun-flink-io-bundle test`.**
- [ ] **Step 5: Commit** `feat(kafka): skip-or-fail policy branch in the routable ingress deserializer`

---

### Task 3: Per-record skip logging (decision 2026-08-08: NO rate limiter)

Design revision over the original ADR text: the operator wants every skipped record individually diagnosable, not a count. Every skip emits one ERROR line with full context; no suppression, no summaries. The ADR paragraph about the rate limiter is rewritten in Task 6 to record this.

**Files:**
- Modify: `RoutableKafkaIngressDeserializer.java` (log from the skip branch directly, SLF4J logger on the class)
- Test: `RoutableKafkaIngressDeserializerTest.java`

**Interfaces:**
- Log line shape (one per skipped record, level from the topic's policy — WARN by default, ERROR when configured):
  `Skipping invalid record: defect [NULL_VALUE], topic [t], partition [0], offset [42], timestamp [1690000000123], key [pk-7], value size [-1]`
  — key segment says `key [none]` for NULL_KEY defects; value size is `-1` for tombstones, byte length otherwise. Same coordinate vocabulary as the stage-1 exception messages so greps work across both policies.

- [ ] **Step 1: Failing tests** — capture the logger (slf4j test appender or logback ListAppender, whichever the module already uses — check existing tests first); assert one event per skipped record with every fragment above, two skips = two events; default policy logs at WARN, `logLevel: error` policy logs the same line at ERROR.
- [ ] **Step 2: Verify RED, 3: implement, 4: green, 5: commit** `feat(kafka): per-record ERROR logging for skipped invalid records`

---

### Task 4: Delegate skips null + metrics

**Files:**
- Modify: `statefun-flink/statefun-flink-io-bundle/src/main/java/org/apache/flink/statefun/flink/io/kafka/KafkaDeserializationSchemaDelegate.java`
- Modify (javadoc only): `statefun-kafka-io/src/main/java/org/apache/flink/statefun/sdk/kafka/KafkaIngressDeserializer.java` — align the javadoc with the now-real contract: "return null to skip the record".
- Test: `statefun-flink/statefun-flink-io-bundle/src/test/java/org/apache/flink/statefun/flink/io/kafka/KafkaDeserializationSchemaDelegateTest.java`

**Interfaces:**
- `deserialize(record, collector)`: null from the delegate deserializer → do NOT collect; increment both counters. Counters registered in `open(DeserializationSchema.InitializationContext context)`: `numInvalidRecordsSkipped` (operator metric group) and standard `numRecordsInErrors`. Guard: metrics may be absent in unit tests (open not called) — counting must no-op, not NPE.

- [ ] **Step 1: Failing tests** — null result is not collected (mock Collector records nothing); non-null still collected; counters incremented once per skipped record after `open`.
- [ ] **Step 2: Verify RED, 3: implement, 4: green, 5: commit** `feat(kafka): delegate skips null-deserialized records and counts them`

---

### Task 5: E2E — default skip keeps the job alive, fail preserves the strict contract

**Files:**
- Modify: `statefun-e2e-tests/statefun-e2e-k8s-native/src/test/java/org/apache/flink/statefun/e2e/k8s/StateFunKafkaInvalidRecordsE2E.java`
- Create: `statefun-e2e-tests/statefun-e2e-k8s-native/src/test/resources/k8s/module-configmap-invalid-fail.yaml` (same module as `module-configmap-invalid.yaml` plus `invalidRecordHandling: {type: fail}` on the ingress)

**Interfaces:**
- Consumes: `Kubectl` helpers and the deployment/topics from PR #291.
- New scenario shape (default deployment, no yaml → skip): send null-key poison, then tombstone poison, then a valid record; assert job state stays RUNNING the whole time, the valid record's result arrives on `counter.results` (filter by unique counterId), and the JM/TM log contains the skip ERROR with coordinates for both defects. No redeploy needed.
- Fatal scenarios: apply `module-configmap-invalid-fail.yaml` + redeploy once, then run the two existing FAILED assertions unchanged; teardown restores the skip ConfigMap.

- [ ] **Step 1: Rewrite the class** — order: skip scenarios first (cheap), then one ConfigMap swap + redeploy, then the two fatal scenarios, teardown restores skip ConfigMap + fresh topic.
- [ ] **Step 2: Compile** `mvn -q -pl :statefun-e2e-k8s-native test-compile -DskipTests`.
- [ ] **Step 3: Commit** `e2e: skip policy keeps the job alive, fail preserves the strict contract`

---

### Task 6: Docs + release notes + full verification

**Files:**
- Modify: `docs/adr/0008-kafka-invalid-record-handling.md` (Status: Proposed → Accepted for skip/fail; forward noted as pending; REWRITE the rate-limiter sentence in the `type: skip` bullet: every skipped record logs individually with full context — defect, topic, partition, offset, timestamp, key, value size — because per-record diagnosability was chosen over flood protection; the metric remains for alerting. Also flip the documented default log level from ERROR to WARN, configurable per ingress/topic via `logLevel: warn|error`)
- Modify: `docs/guides/kafka-io.md` — new "Invalid records" section documenting the yaml, the default change, the metrics, and the log shape
- Modify: release notes / changelog location used by the repo (check `docs/release-process.md` for where breaking changes are recorded)

- [ ] **Step 1: Write docs.**
- [ ] **Step 2: Full local verify** — `mvn -pl statefun-flink/statefun-flink-io-bundle test` and `mvn -q -pl :statefun-e2e-k8s-native test-compile`.
- [ ] **Step 3: Commit** `docs: invalid record handling guide, ADR-0008 accepted for skip/fail`
- [ ] **Step 4: Push, open PR referencing ADR-0008 + issue #272, watch the E2E gate.**
