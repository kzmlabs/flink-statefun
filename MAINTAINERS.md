# Maintainers

This project is actively maintained by Kzmlabs.

| Maintainer | GitHub | Role |
|------------|--------|------|
| Oleksandr Kazimirov | [@oleksandr-kazimirov](https://github.com/oleksandr-kazimirov) | Lead maintainer, releases |

## Releases

- All releases are signed (GPG via Maven Central, Sigstore keyless via GHCR)
- Release tags follow the pattern `v3.4.0-KZM-X.Y[-RCN]` (full release) or `docker-3.4.0-KZM-X.Y` (Docker-only)
- Maven Central namespace: `io.github.kzmlabs.flinkstatefun`
- Container registry: `ghcr.io/kzmlabs/flink-statefun`

## Reporting issues

- **Security vulnerabilities**: see [SECURITY.md](SECURITY.md) — private disclosure via GitHub Security Advisories
- **Bugs / features**: open an issue using the templates in `.github/ISSUE_TEMPLATE/`

## Contribution

Pull requests welcome. See [CONTRIBUTING.md](CONTRIBUTING.md).

## Project heritage

Kzmlabs Flink StateFun is a downstream continuation of [Apache Flink Stateful Functions](https://github.com/apache/flink-statefun). Apache 2.0 licensed; copyright attribution to the Apache Software Foundation is preserved at the file level (SPDX) and in the project [`NOTICE`](NOTICE) at root.
