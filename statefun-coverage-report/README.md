# statefun-coverage-report

Aggregates JaCoCo code coverage across every production module in the reactor and produces a single project-wide report.

This module has **no source code** and produces **no published artifact**. Its only job is to run JaCoCo's `report-aggregate` goal in the `verify` phase, after every other module has written its own `target/jacoco.exec` file.

---

## End-to-end flow

How a single `mvn install` run turns into a coverage number, from JVM agent injection to GitHub Actions UI:

```mermaid
flowchart TB
    subgraph "Reactor build (per production module)"
        A[mvn verify] --> B[jacoco:prepare-agent]
        B -->|sets ${argLine} to<br/>-javaagent:jacocoagent.jar| C[surefire:test]
        C -->|forks JVM with agent attached| D[Tests run<br/>agent instruments classes<br/>as they load]
        D -->|on JVM exit| E[target/jacoco.exec<br/>binary trace data]
        E --> F[jacoco:report<br/>verify phase]
        F --> G[target/site/jacoco/<br/>per-module HTML + XML]
    end

    subgraph "statefun-coverage-report (LAST in reactor)"
        H[jacoco:report-aggregate<br/>verify phase] -->|walks ${project.build.directory}<br/>of every dependency module| I[Reads each module's<br/>target/jacoco.exec<br/>+ target/classes]
        I --> J[Merges + filters<br/>via class-level excludes]
        J --> K[target/site/jacoco-aggregate/<br/>jacoco.xml + index.html]
    end

    G -.->|all per-module .exec files exist| H

    subgraph "CI consumption (.github/workflows/ci.yml)"
        K --> L[Sanity check:<br/>grep '&lt;counter ' jacoco.xml]
        L -->|empty = loud failure| M[Job summary:<br/>markdown table in run UI]
        L --> N[Upload HTML artifact<br/>retention: 30 days]
        L --> O[Upload jacoco.xml<br/>to Codecov via OIDC]
    end

    style B fill:#e1f5ff
    style F fill:#e1f5ff
    style H fill:#fff4e1
    style L fill:#ffe1e1
```

**Key invariants:**

- The aggregator is the **last module in the reactor** (root `pom.xml` `<modules>`). Maven processes modules in declared order, so `report-aggregate` runs only after every dependency module has written its `jacoco.exec`.
- The agent JVM flag is injected via the parent pom's `surefire` `<argLine>@{argLine} -Xms256m...</argLine>` Maven late-binding. **Without `@{argLine}` the literal value silently swallows the agent**, producing an empty report with no error. The CI sanity check (`grep '<counter '`) turns this silent failure into a loud build failure.

---

## How E2E tests fit in (and why they don't)

E2E tests **do not contribute coverage**, by design. Three independent mechanisms enforce this:

```mermaid
flowchart LR
    subgraph "Production module (e.g. statefun-flink-core)"
        P1[parent pom<br/>jacoco-prepare-agent execution<br/>jacoco.skip = false] -->|injects agent| P2[surefire test JVM<br/>argLine = -javaagent:...]
        P2 --> P3[target/jacoco.exec<br/>WRITTEN]
    end

    subgraph "E2E module (e.g. statefun-smoke-e2e-driver)"
        E1[statefun-e2e-tests/pom.xml<br/>cascades jacoco.skip = true] -.->|skips| E2[jacoco-prepare-agent<br/>NO-OP]
        E2 --> E3[surefire/failsafe test JVM<br/>argLine = empty]
        E3 --> E4[NO target/jacoco.exec written]
    end

    P3 -->|listed as dep<br/>in aggregator| AGG[report-aggregate]
    E4 -.->|NOT listed as dep<br/>in aggregator| AGG

    AGG --> RPT[Coverage % reflects<br/>unit tests only]

    style E1 fill:#ffe1e1
    style E4 fill:#ffe1e1
    style P3 fill:#e1ffe1
```

**Three layers preventing E2E from polluting coverage:**

1. **`<jacoco.skip>true</jacoco.skip>`** in `statefun-e2e-tests/pom.xml` cascades to all 10 child modules. The `prepare-agent` execution becomes a no-op, so no `-javaagent` flag is injected into the test JVM.
2. **No `jacoco.exec` is written** for any E2E module. Even if `report-aggregate` tried to scan them (it doesn't), there would be no data.
3. **The aggregator's `<dependencies>` block does not list any `statefun-e2e-tests/*` module**. `report-aggregate` walks declared dependencies only, so E2E modules are never inspected.

**Cross-JVM isolation matters too.** When `statefun-smoke-e2e-driver` boots a Flink job that exercises code in `statefun-flink-core`, the executions happen in **separate JVMs** (Testcontainers spawns containers; kind launches Flink JM/TM pods). Those JVMs don't have the agent attached, so `statefun-flink-core`'s coverage data is unaffected by E2E activity.

**Net effect**: the headline coverage number is **honest unit-test reach** — not "tested by anything". E2E tests still run (validate end-to-end behavior, fail loudly on regressions), they just don't show up as covered lines in the report.

### When E2E coverage WOULD be added

Three trigger conditions, documented in [issue #149 §E2E](https://github.com/kzmlabs/flink-statefun/issues/149):

1. A module's unit-test coverage plateaus low (<40%) and the gap is provably in E2E-only code paths (Kinesis source bootstrap, K8s manifest binder, module-loader classpath scanning).
2. A regression slips through unit tests but is caught by E2E.
3. Apache Flink upstream ships a coverage-friendly E2E harness pattern.

Until then, the multi-day plumbing required to extract `.exec` files from kind-cluster pods is not worth the marginal gain (<5% across the project, mostly already covered by unit tests).

---

## Two-layer exclusion model

```mermaid
flowchart TB
    M[All modules in reactor] --> CHK{Module type?}

    CHK -->|Production code| P[prepare-agent active<br/>jacoco.exec written]
    CHK -->|Test scaffolding<br/>statefun-testutil<br/>statefun-e2e-tests/*| S1[<jacoco.skip>true</jacoco.skip><br/>module-level skip]
    CHK -->|Aggregator self<br/>statefun-coverage-report| S2[<jacoco.skip>true</jacoco.skip><br/>only report-aggregate runs]

    P --> CL{Class matches<br/>plugin-level excludes?}
    CL -->|Yes:<br/>**/generated/**<br/>**/*Proto*.class<br/>**/*OuterClass*.class<br/>org/.../sdk/shaded/**| EX[Excluded from instrumentation<br/>AND from report]
    CL -->|No| INC[Instrumented<br/>counted in jacoco.xml]

    S1 --> SKIP[No jacoco.exec written<br/>NOT in aggregator deps<br/>invisible in report]

    style EX fill:#ffe1e1
    style SKIP fill:#ffe1e1
    style INC fill:#e1ffe1
```

**Why two layers (module-level skip + class-level exclude)?**

| Approach | What it does | Used for |
|---|---|---|
| **Module-level `<jacoco.skip>true</jacoco.skip>`** | Disables the agent entirely; module vanishes from per-module view | Test scaffolding only — these modules have no production code worth measuring |
| **Class-level `<excludes>` in plugin config** | Agent ignores matching classes during instrumentation; they show as 0/0 in per-module view, not "vanished" | Generated protobuf code, shaded/relocated bytecode in production modules — the *module* matters (may grow real code later), the *classes* don't |

This matters for `statefun-flink-runner`, `statefun-sdk-protos`, and `statefun-shaded/*`: today they're assembly/generated/shaded with no instrumentable code. **They are still listed in the aggregator's `<dependencies>`** so they appear as N/A in the per-module view rather than vanishing. If a future PR adds real code to one of them, the per-module view immediately shows the new uncovered lines instead of silently bypassing coverage tracking.

`statefun-bom` and `statefun-docker` are NOT in the aggregator dependencies — they have no JAR / no Java code at all, so there's nothing for `report-aggregate` to walk.

---

## Three publication guards

The aggregator must never be published to Maven Central or Docker Hub. Four independent mechanisms enforce this — any one failing still blocks publication:

```mermaid
flowchart LR
    PUB{Try to publish<br/>statefun-coverage-report?} --> G1{central.skip<br/>= true?}
    G1 -->|yes| BLOCK1[Sonatype Central plugin<br/>refuses upload]
    G1 -.->|no| G2{maven.deploy.skip<br/>= true?}
    G2 -->|yes| BLOCK2[maven-deploy-plugin<br/>refuses upload]
    G2 -.->|no| G3{maven.install.skip<br/>= true?}
    G3 -->|yes| BLOCK3[maven-install-plugin<br/>refuses local install]
    G3 -.->|no| G4{In release.yml<br/>-pl exclusion?}
    G4 -->|yes| BLOCK4[Reactor never<br/>runs deploy here]
    G4 -.->|no| LEAK[would publish]

    style BLOCK1 fill:#e1ffe1
    style BLOCK2 fill:#e1ffe1
    style BLOCK3 fill:#e1ffe1
    style BLOCK4 fill:#e1ffe1
    style LEAK fill:#ffe1e1
```

`<central.skip>` only covers the Sonatype plugin path. `maven-deploy-plugin` and `maven-install-plugin` are independent and would still try to push to whatever `<distributionManagement>` resolves to if the `-pl` exclusion were ever edited. The four-guard design means an aggregator pom edit that accidentally removes one guard still leaves three intact.

---

## Where to view the results

| Location | What you get | Latency from CI run |
|---|---|---|
| **GitHub Actions run summary** | Markdown table with all 6 JaCoCo metric counters (Instruction, Branch, Line, Complexity, Method, Class) | Visible immediately in the run UI |
| **`coverage-report-html` artifact** on the run page | Full drillable JaCoCo HTML site (per-module → package → class → line-level red/green) — download ZIP, open `index.html` | Available as soon as the upload step completes (~10 sec after build) |
| **Codecov dashboard** at `app.codecov.io/gh/kzmlabs/flink-statefun` | Trend graphs, PR coverage delta comments, per-module breakdown, badge URL | ~30 sec after CI uploads (Phase 2 — optional, third-party) |
| **Local `mvn install`** | `statefun-coverage-report/target/site/jacoco-aggregate/index.html` | Immediate — same as the CI artifact, just generated locally |

---

## Skip flag reference

| Module / pattern | `jacoco.skip` | In aggregator deps | Reason |
|---|---|---|---|
| `statefun-flink/*` (rich tests) | false | yes | Production code with unit tests — primary signal |
| `statefun-sdk-java`, `statefun-sdk-embedded` | false | yes | SDK code with unit tests |
| `statefun-kafka-io`, `statefun-kinesis-io` | false | yes | Connector code with unit tests |
| `statefun-sdk-protos` | false | yes | Generated protobuf — class-level excludes drop content; appears as N/A |
| `statefun-flink-runner` | false | yes | Assembly module today; future-proof for code growth |
| `statefun-shaded/*` | false | yes | Relocated bytecode — class-level excludes drop content; appears as N/A |
| `statefun-bom` | n/a | **no** | No JAR, no `.class` files; nothing to walk |
| `statefun-docker` | n/a | **no** | Docker assembly only, no Java |
| `statefun-testutil` | **true** | no | Test scaffolding only |
| `statefun-e2e-tests/*` (10 modules) | **true** | no | E2E in containers; see flow diagram above |
| `statefun-coverage-report` (this module) | **true** | n/a | Aggregator itself; only `report-aggregate` execution runs |
