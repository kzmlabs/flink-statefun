# Contributing to Kzmlabs Flink StateFun

Thanks for your interest in contributing! This project is an actively maintained continuation of the Stateful Functions framework, updated for Flink 2.2.0 and Java 21.

## Ways to Contribute

- **Report bugs** — Open an issue with a minimal reproducer, Flink/Java versions, and stack trace
- **Request features** — Open an issue describing the use case and desired behavior
- **Submit pull requests** — Bug fixes, new ingress/egress connectors, documentation, tests
- **Improve documentation** — README, inline Javadoc, examples
- **Answer questions** — Help others in issues and discussions

## Development Setup

### Prerequisites

- Java 21 (`JAVA_HOME` set)
- Maven 3.5+
- Docker (for K8s E2E tests)

### Build

```bash
# Full build with all tests (including K8s E2E)
mvn install -B

# Skip K8s E2E for faster iteration
mvn install -Dskip.k8s.e2e -B

# Skip all tests
mvn install -DskipTests -B
```

### Code Style

Google Java Format (2-space indent) is enforced by `spotless-maven-plugin`. Before committing:

```bash
mvn spotless:apply
```

CI will fail builds that do not conform.

## Pull Request Process

1. Fork the repository and create a feature branch from `release`
2. Make focused, well-scoped commits with clear messages
3. Add or update tests for behavior changes (JUnit Jupiter 5.11)
4. Run `mvn spotless:apply` before committing
5. Ensure `mvn install -Dskip.k8s.e2e -B` passes locally
6. Open a PR against the `release` branch
7. A maintainer will review; CI must pass before merge

### Commit Messages

- Use imperative mood ("Add Kafka headers support", not "Added")
- First line ≤ 72 characters
- Reference issue numbers where relevant (`Fixes #123`)

## Reporting Bugs

Include:

- Kzmlabs StateFun version
- Flink version (should be 2.2.x)
- Java version (should be 21)
- OS and architecture
- Minimal reproducer (code + config)
- Full stack trace and relevant logs

## License

By contributing, you agree your contributions will be licensed under the [Apache License 2.0](LICENSE).
