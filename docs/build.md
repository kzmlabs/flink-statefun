---
title: Build from source
description: Build StateFun Actors (Apache Flink 2.2 + Java 21) from source — full reactor, fast iteration modes, restricted-network registry mirrors, formatter, and contribution workflow.
---

# Build from source

> Full reactor in 25–30 minutes, compile-only in 3–5. Works on Linux, macOS, Windows. Restricted-network friendly.

## Prerequisites

- **Java 21** — Temurin, Liberica, Corretto, or any 21 LTS distribution
- **Maven 3.9+** — or use the bundled `./mvnw` wrapper (no install required)
- **Docker** — only required for the Kubernetes E2E gate

Verify your environment:

```bash
java -version    # should report 21
./mvnw -v        # should report Maven 3.9+
docker info      # should connect to a running daemon
```

## Standard build

```bash
git clone https://github.com/kzmlabs/flink-statefun.git
cd flink-statefun
./mvnw clean install
```

This builds every module and runs every test, including the K8s E2E gate (kind cluster + Flink Operator + Kafka + LocalStack + real integration tests). Total: **~25–30 min** on a developer laptop.

## Faster iteration

=== "Skip K8s E2E"

    Skips kind-cluster provisioning + integration tests. Keeps unit tests.

    ```bash
    ./mvnw clean install -Dskip.k8s.e2e
    ```

    **~5–7 min**

=== "Skip all tests"

    Compile + package only.

    ```bash
    ./mvnw clean install -DskipTests
    ```

    **~3–5 min**

=== "Single module"

    Builds the named module and the modules it depends on (`-am`).

    ```bash
    ./mvnw -pl :statefun-sdk-java -am verify
    ```

=== "Just the K8s E2E"

    Re-run the E2E gate after a code change without rebuilding upstream modules.

    ```bash
    ./mvnw verify -pl :statefun-e2e-k8s-native -am -Dskip.teardown=true
    ```

    `-Dskip.teardown=true` keeps the kind cluster after the test for `kubectl` debugging.

## Restricted-network builds

Every Dockerfile and Kubernetes manifest in the build is parametric so you can pull base images through an internal registry mirror — no fork required.

```bash
export IMAGE_REGISTRY_PREFIX=harbor.example.com/dockerhub-proxy/
./mvnw clean install
```

The prefix is honoured by:

- `statefun-docker/src/main/docker/Dockerfile` — the runtime distribution image
- `statefun-e2e-tests/statefun-e2e-k8s-native/remote-function/Dockerfile` — the test fixture image
- `kafka.yaml` and `localstack.yaml` k8s manifests applied during E2E setup

Default empty pulls directly from Docker Hub — no behaviour change for the typical case.

## Code formatting

The codebase follows **Google Java Format** (2-space indent). There is no formatter plugin in the build; format via your IDE's google-java-format integration (or the standalone [google-java-format](https://github.com/google/google-java-format) tool) before committing, and match the surrounding style when editing existing files.

## Contribution workflow

1. Branch from `release` (the active development branch).
2. Make changes; run `./mvnw -Dskip.k8s.e2e install` locally.
3. Open a PR against `release`. CI runs the full K8s E2E gate, CodeQL, Scorecard, Trivy, and dep-convergence enforcer.
4. Merge via the merge queue once green.

!!! warning "PR target branch"

    PRs targeting `master` are not accepted. `master` is a vestigial Apache fork pointer; all development happens on `release`.

## Troubleshooting

??? failure "Build fails on `enforcer:dependencyConvergence`"

    Two paths bring different versions of the same artifact onto the classpath. Add a pin in the root pom's `<dependencyManagement>` — see existing pins for `commons-lang3`, `jsr305`, `flink-shaded-netty` as templates.

??? failure "K8s E2E hangs on `kind create cluster`"

    Docker is out of disk or the daemon is unresponsive. Check `df -h`, `docker system df`, and consider `docker system prune -af --volumes`.

??? failure "`mvn verify` reports `ClassNotFoundException` after rebuild"

    Stale `.m2` cache after a `versions:set` or large refactor. Force a fresh resolve:

    ```bash
    ./mvnw clean install -DskipTests -U
    ```

## Next steps

<div class="grid cards" markdown>

- :material-rocket-launch:{ .lg .middle } &nbsp; **[Quickstart](quickstart.md)** — verify your build works against a live cluster.
- :material-test-tube:{ .lg .middle } &nbsp; **[E2E test architecture](architecture/e2e-tests.md)** — what the K8s gate exercises and why.
- :material-source-branch-sync:{ .lg .middle } &nbsp; **[Release process](release-process.md)** — how versions get cut and shipped.

</div>
