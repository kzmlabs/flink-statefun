# Kzmlabs StateFun Release Guide

## Overview

This guide documents the release process for publishing to Maven Central and GitHub Container Registry (GHCR).

## Current Release: 3.4.0-KZM-2.0-RC7

- **Maven Central**: Published
- **Docker Image**: `ghcr.io/kzmlabs/flink-statefun:3.4.0-KZM-2.0-RC7`
- **GitHub Release**: https://github.com/kzmlabs/flink-statefun/releases

---

## Release Workflows

### 1. Full Release (Maven Central + Docker + GitHub Release)

Triggered automatically when pushing a tag starting with `v`:

```bash
# Update version
mvn versions:set -DnewVersion=3.4.0-KZM-2.0-RC8 -DgenerateBackupPoms=false

# Commit and tag
git add -A
git commit -m "Release 3.4.0-KZM-2.0-RC8"
git push origin release
git tag v3.4.0-KZM-2.0-RC8
git push origin v3.4.0-KZM-2.0-RC8
```

This runs `.github/workflows/release.yml` which:
1. Builds the project
2. Deploys to Maven Central (with GPG signing)
3. Builds and pushes Docker image to GHCR
4. Creates GitHub Release

### 2. Docker Only Release (No Maven Central)

Triggered by pushing a tag starting with `docker-`. Uses the same `release.yml` workflow
but skips Maven Central deploy and GitHub Release creation.

```bash
# Tag for Docker-only release
git tag docker-3.4.0-KZM-2.0-test1
git push origin docker-3.4.0-KZM-2.0-test1
```

This pushes the image as `ghcr.io/kzmlabs/flink-statefun:3.4.0-KZM-2.0-test1`.

There is also a `docker-release.yml` workflow with `workflow_dispatch` for manual triggers via the GitHub Actions UI.

---

## Maven Central Requirements

Each module published to Maven Central MUST have:

### 1. Required POM Elements
```xml
<name>module-name</name>  <!-- REQUIRED - was missing, caused "Project name is missing" -->
<description>...</description>  <!-- Inherited from parent -->
<url>...</url>  <!-- Inherited from parent -->
<licenses>...</licenses>  <!-- Inherited from parent -->
<developers>...</developers>  <!-- Inherited from parent -->
<scm>...</scm>  <!-- Inherited from parent -->
```

### 2. Required Artifacts
- Main JAR
- Sources JAR (`-sources.jar`)
- Javadoc JAR (`-javadoc.jar`)
- GPG signatures (`.asc` files)

### 3. Modules Without Java Sources

For modules without Java sources (e.g., `statefun-sdk-protos`, `statefun-flink-distribution`), add empty javadoc jar:

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-jar-plugin</artifactId>
    <executions>
        <execution>
            <id>empty-javadoc-jar</id>
            <phase>package</phase>
            <goals>
                <goal>jar</goal>
            </goals>
            <configuration>
                <classifier>javadoc</classifier>
                <classesDirectory>${project.basedir}/src/main/resources</classesDirectory>
            </configuration>
        </execution>
    </executions>
</plugin>
```

---

## Modules Excluded from Maven Central

These modules are excluded via `-pl` in the release workflow:

- `statefun-e2e-tests` (and all submodules)
- `statefun-sdk-python`
- `statefun-sdk-go`
- `statefun-sdk-js`

They have `<central.skip>true</central.skip>` in their pom.xml.

---

## CI/CD Configuration

### Maven Central Publishing Plugin

In root `pom.xml` under `release` profile:

```xml
<plugin>
    <groupId>org.sonatype.central</groupId>
    <artifactId>central-publishing-maven-plugin</artifactId>
    <version>0.6.0</version>
    <extensions>true</extensions>
    <configuration>
        <publishingServerId>central</publishingServerId>
        <autoPublish>true</autoPublish>
        <waitUntil>published</waitUntil>  <!-- Makes CI fail if validation fails -->
        <waitMaxTime>600</waitMaxTime>
        <checksums>required</checksums>
        <skipPublishing>${central.skip}</skipPublishing>
    </configuration>
</plugin>
```

### Required Secrets

Set these in GitHub repository settings:

| Secret | Description |
|--------|-------------|
| `OSSRH_USERNAME` | Sonatype Central username |
| `OSSRH_PASSWORD` | Sonatype Central password/token |
| `GPG_PRIVATE_KEY` | GPG private key for signing |
| `GPG_PASSPHRASE` | GPG key passphrase |

---

## Troubleshooting

### "Project name is missing"
Add `<name>module-name</name>` to the module's pom.xml.

### "Javadocs must be provided but not found"
Module has no Java sources. Add empty javadoc jar plugin (see above).

### CI succeeds but Maven Central validation fails
Check Sonatype portal: https://central.sonatype.com/publishing/deployments

The `waitUntil=published` setting should make CI fail on validation errors.

### Docker pull unauthorized
Make package public: https://github.com/orgs/kzmlabs/packages/container/flink-statefun/settings

---

## Version History

| Version | Status | Notes |
|---------|--------|-------|
| RC20 | ✅ Published | Fixed all Maven Central validation issues |
| RC19 | ❌ Failed | Missing javadoc for statefun-flink-distribution |
| RC18 | ❌ Failed | Missing `<name>` in all modules |
| RC17 | ❌ Failed | Missing javadoc for statefun-sdk-protos |

---

## Quick Commands

```bash
# Check current version
grep -m1 "<version>" pom.xml

# Update version
mvn versions:set -DnewVersion=X.Y.Z -DgenerateBackupPoms=false

# Build locally
mvn clean install -DskipTests

# Build with release profile (requires GPG)
mvn clean install -Prelease -DskipTests -Dgpg.skip=true

# Check workflow status
gh run list --repo kzmlabs/flink-statefun

# Pull Docker image
docker pull ghcr.io/kzmlabs/flink-statefun:3.4.0-KZM-2.0-RC7
```
