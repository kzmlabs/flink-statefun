# Building from source

## Prerequisites

- **Java 21** (Temurin, Liberica, or Corretto)
- **Maven 3.9+** (or use the bundled `mvnw` wrapper)
- **Docker** (required only for the Kubernetes E2E gate)

## Standard build

```bash
git clone https://github.com/kzmlabs/flink-statefun.git
cd flink-statefun
./mvnw clean install
```

This builds every module and runs every test, including the K8s E2E gate (which provisions a `kind` cluster, deploys the Flink Operator + Kafka + LocalStack, and runs the integration tests against a real cluster). Total time: ~25–30 minutes on a developer laptop.

## Faster iterations

=== "Skip K8s E2E"

    ```bash
    ./mvnw clean install -Dskip.k8s.e2e
    ```

    Skips the kind-cluster provisioning + integration tests. ~5–7 minutes.

=== "Skip all tests"

    ```bash
    ./mvnw clean install -DskipTests
    ```

    Compile + package only. ~3–5 minutes.

=== "Single module"

    ```bash
    ./mvnw -pl :statefun-sdk-java -am verify
    ```

    Builds the requested module and its required dependencies (`-am`).

## Restricted-network builds

The K8s E2E manifests and Dockerfiles are parametric for downstream consumers behind an internal mirror.

```bash
# Pull all base images through your registry mirror
export IMAGE_REGISTRY_PREFIX=harbor.example.com/dockerhub-proxy/
./mvnw clean install
```

`IMAGE_REGISTRY_PREFIX` is honoured by:

- `statefun-docker/src/main/docker/Dockerfile`
- `statefun-e2e-tests/statefun-e2e-k8s-native/remote-function/Dockerfile`
- The Kafka and LocalStack k8s manifests applied during E2E setup

Default empty pulls directly from Docker Hub.

## Code formatting

The project enforces Google Java Format with two-space indentation via Spotless:

```bash
./mvnw spotless:apply -pl <module>
```

Run before committing — the build fails on formatting drift.

## Submitting changes

1. Branch from `release` (the active development branch).
2. Run `./mvnw spotless:apply` and `./mvnw -Dskip.k8s.e2e install` locally.
3. Open a PR against `release`. CI will run the full K8s E2E gate.
4. PRs targeting `master` are not accepted; `master` is a vestigial Apache fork pointer.
