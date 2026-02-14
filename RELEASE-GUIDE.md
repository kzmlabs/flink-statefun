# Kzmlabs StateFun Release Guide

## Overview

This guide documents the release process for publishing to Maven Central and GitHub Container Registry (GHCR).

## Current Release: 3.4.0-KZM-1.0-RC20

- **Maven Central**: Published
- **Docker Image**: `ghcr.io/kzmlabs/flink-statefun:3.4.0-KZM-1.0-RC20`
- **GitHub Release**: https://github.com/kzmlabs/flink-statefun/releases

---

## Release Workflows

### 1. Full Release (Maven Central + Docker + GitHub Release)

Triggered automatically when pushing a tag starting with `v`:

```bash
# Update version
mvn versions:set -DnewVersion=3.4.0-KZM-1.0-RC21 -DgenerateBackupPoms=false

# Commit and tag
git add -A
git commit -m "Release 3.4.0-KZM-1.0-RC21"
git push origin release
git tag v3.4.0-KZM-1.0-RC21
git push origin v3.4.0-KZM-1.0-RC21
```

This runs `.github/workflows/release.yml` which:
1. Builds the project
2. Deploys to Maven Central (with GPG signing)
3. Builds and pushes Docker image to GHCR
4. Creates GitHub Release

### 2. Docker Only Release (No Maven Central)

Create a new workflow file `.github/workflows/docker-release.yml`:

```yaml
name: Docker Release Only

on:
  workflow_dispatch:
    inputs:
      version:
        description: 'Version tag for Docker image'
        required: true
        default: 'latest'

env:
  REGISTRY: ghcr.io
  IMAGE_NAME: kzmlabs/flink-statefun

jobs:
  docker:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      packages: write

    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Set up JDK 21
        uses: actions/setup-java@v4
        with:
          java-version: '21'
          distribution: 'temurin'
          cache: 'maven'

      - name: Build
        run: mvn clean install -DskipTests -B

      - name: Set up Docker Buildx
        uses: docker/setup-buildx-action@v3

      - name: Log in to GitHub Container Registry
        uses: docker/login-action@v3
        with:
          registry: ${{ env.REGISTRY }}
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}

      - name: Prepare Docker build context
        run: |
          DISTRIBUTION_JAR=$(find . -path "*/statefun-flink-distribution/target/statefun-flink-distribution*.jar" -not -name "original-*" -not -name "*sources*" -not -name "*javadoc*" | head -1)
          CORE_JAR=$(find . -path "*/statefun-flink-core/target/statefun-flink-core*.jar" -not -name "*javadoc*" -not -name "*sources*" | head -1)

          mkdir -p docker-context/flink/lib
          cp -r tools/docker/flink-distribution-template/* docker-context/flink/
          cp "$DISTRIBUTION_JAR" docker-context/flink/lib/statefun-flink-distribution.jar
          cp "$CORE_JAR" docker-context/flink/lib/statefun-flink-core.jar
          cp tools/docker/Dockerfile docker-context/
          cp tools/docker/docker-entry-point.sh docker-context/

      - name: Build and push Docker image
        uses: docker/build-push-action@v5
        with:
          context: docker-context
          push: true
          tags: |
            ${{ env.REGISTRY }}/${{ env.IMAGE_NAME }}:${{ github.event.inputs.version }}
          labels: |
            org.opencontainers.image.title=Kzmlabs Flink StateFun
            org.opencontainers.image.description=Kzmlabs fork of Apache Flink Stateful Functions
            org.opencontainers.image.version=${{ github.event.inputs.version }}
            org.opencontainers.image.source=${{ github.server_url }}/${{ github.repository }}
```

**To trigger Docker-only release:**
1. Go to Actions → "Docker Release Only"
2. Click "Run workflow"
3. Enter version tag (e.g., `3.4.0-KZM-1.0-RC21-docker`)
4. Click "Run workflow"

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
docker pull ghcr.io/kzmlabs/flink-statefun:3.4.0-KZM-1.0-RC20
```
