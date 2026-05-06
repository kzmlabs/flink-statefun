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

### Testing conventions

The project uses **JUnit 5 Jupiter** with **AssertJ** for fluent assertions. Hamcrest is also available for legacy tests. Match the style of the surrounding tests in the module you are touching.

**Test method naming**

Use plain camelCase, no underscores. Make the name a short statement of the property under test. The class name (`SomethingTest`) already provides the subject; the method name describes the behavior:

```java
// Good — matches the project's existing 300+ tests
@Test
void routesArnKeyedRecordsToConfiguredTargets() { ... }

@Test
void throwsWhenStreamKeyIsNotInRoutingMap() { ... }

// Avoid — snake_case is inconsistent with the rest of the codebase
@Test
void deserialize_withArnAsStream_routesToConfiguredTargets() { ... }
```

Do not use `@DisplayName`. The test method name is the documentation; relying on annotations splits the description from the code that defines it.

**Test structure**

- Arrange / Act / Assert separation visible in each test.
- One concept per test (`@ParameterizedTest` for the same property across multiple inputs).
- Use `@BeforeEach` to set up shared state; tests that need a different setup shadow with a local variable instead of mutating the shared field.
- Real types over mocks where practical. Reach for mocks only when the dependency is genuinely external (network, filesystem) or has a complex lifecycle.
- Helpers (record builders, config builders) as `private static` methods at the bottom of the test class.

**Documentation**

Use Javadoc on tests only when the *why* is non-obvious — e.g. "regression guard for the Flink 2.x ARN-vs-short-name contract", not "tests deserialization". The reader can see what the test does from the assertions; the comment should add context the assertions can't.

**Coverage guidance**

The project tracks coverage via JaCoCo + Codecov ([flags `unittests` and `e2e`](https://app.codecov.io/gh/kzmlabs/flink-statefun)). New behavior should be covered. Don't chase coverage percentages on glue/getter/builder code; prioritize critical-runtime paths (binders, state access, message dispatch) and error paths (boundary conditions, malformed input, transient failures). See [issue #149](https://github.com/kzmlabs/flink-statefun/issues/149) for the rolling investment plan.

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
