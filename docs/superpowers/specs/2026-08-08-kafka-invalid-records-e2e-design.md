# Kafka invalid-record E2E scenarios — design

Date: 2026-08-08
Status: approved
Related: ADR-0008 (invalid record handling), stage-1 diagnostics on branch `fix/kafka-ingress-invalid-record-context`

## Goal

Cover failing Kafka-record deserialization on the routable ingress with dedicated, isolated K8s-native E2E scenarios, structured so that future `invalidRecordHandling` strategies (`skip`, `forward`) extend the same test class instead of requiring new infrastructure.

## Constraints

- Invalid records are currently job-fatal, so scenarios must not share a FlinkDeployment with the existing ordered tests in `StateFunK8sE2E`.
- Each fatal scenario kills the job, so consecutive fatal scenarios need a redeploy in between (~1-2 min in kind).
- Keep it very simple and transparent: no scenario framework, plain test methods, part of the same `mvn verify` E2E scope and PR gate. Accepted gate cost: +2-4 min.

## Design

### 1. Isolation: a second, minimal FlinkDeployment

- `src/test/resources/k8s/flink-deployment-invalid.yaml`: FlinkDeployment `statefun-e2e-invalid`, 1 TM, same StateFun image as the main deployment.
- `src/test/resources/k8s/module-configmap-invalid.yaml`: one routable Kafka ingress on topic `invalid.commands` targeting the existing counter function (served by the shared remote-function pod), Kafka egress to `invalid.results`.
- `scripts/setup-cluster.sh`: create topics `invalid.commands` / `invalid.results`, apply the CR, wait READY (same wait pattern as the main deployment).

Killing this job never affects the main suite; the main job never consumes these topics.

### 2. Test class `StateFunKafkaInvalidRecordsE2E`

`@Tag("kafka")`, same module `statefun-e2e-k8s-native`. Every scenario is one plain test method with the shape "produce a defect record, then assert the observable outcome". Ordered methods, current scenarios:

1. `nullKeyRecordFailsJobWithRecordCoordinates` — produce a record with a null key and a valid payload; await the invalid-deployment JobManager log containing `requires a UTF-8 key`, `topic [invalid.commands]`, `partition [`, `offset [`, and the FlinkDeployment reaching job state FAILED. This pins the stage-1 diagnostics end to end.
2. Redeploy helper between fatal scenarios: delete CR, re-apply, wait READY.
3. `tombstoneRecordFailsJobWithRecordCoordinatesAndKey` — produce a valid-key record with a null value; await the `tombstone` diagnostic including the `key [...]` segment, and FAILED state.

Helpers follow the existing `KubectlPortForward` ProcessBuilder pattern, kept as small static methods (or a tiny `Kubectl` util): `jobManagerLog(deployment)`, `deploymentJobState(name)`, `redeploy(name)`.

### 3. Extension contract for future strategies

When `invalidRecordHandling: skip` ships: switch (or add a variant of) the invalid module ConfigMap to the desired policy and add cheap non-fatal methods — "poison record, job stays RUNNING, a follow-up valid record is still processed, skip log/metric observed" — no redeploy needed. The current fatal scenarios then move to an explicit `type: fail` ingress, preserving coverage of the strict contract. New scenario = new method; new strategy = new ConfigMap variant. No other machinery.

## Out of scope

- Kinesis scenarios.
- Deserialization-failure defect class (malformed payload bytes): the routable ingress wraps payloads opaquely today, so there is no E2E-visible deser failure until ADR-0008 stage 2 defines one; add the scenario then.
- Metrics assertions (arrive with the skip policy).
