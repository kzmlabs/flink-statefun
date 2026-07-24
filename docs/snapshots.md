---
title: Snapshots
description: How to consume StateFun Actors snapshot builds from GitHub Packages — repository configuration, authentication, and the maintainer publish flow.
---

# Using snapshot builds

Stable releases ship to **Maven Central** (`central.sonatype.com`). Development snapshots between releases ship to **GitHub Packages** under the `kzmlabs/flink-statefun` repository.

## Why two channels

| Channel | Used for | Why |
|---|---|---|
| Maven Central | Stable releases (`v*` tags) | Industry-standard public discovery, no consumer auth required |
| GitHub Packages | Snapshots (`snapshot-*` tags) | Zero per-namespace permission gates, uses the kzmlabs org's existing GitHub auth |

Sonatype Central Portal blocks snapshot publishing to `io.github.kzmlabs.flinkstatefun` with HTTP 403 absent a namespace-level toggle that isn't currently exposed in the UI. Routing snapshots to GitHub Packages is the friction-free alternative — release credibility (Maven Central) is reserved for stable artifacts where it matters most.

## Consumer setup

Snapshots are not normally needed unless you're testing in-progress changes. For stable production use, just consume the latest Maven Central version and skip the rest of this page.

### 1. Add the GitHub Packages repository to your `pom.xml`

```xml
<repositories>
    <repository>
        <id>kzmlabs-snapshots</id>
        <url>https://maven.pkg.github.com/kzmlabs/flink-statefun</url>
        <snapshots>
            <enabled>true</enabled>
        </snapshots>
        <releases>
            <enabled>false</enabled>
        </releases>
    </repository>
</repositories>
```

### 2. Authenticate to GitHub Packages

GitHub Packages requires authentication even for public repositories. Generate a Personal Access Token at https://github.com/settings/tokens (classic) with the `read:packages` scope only — no other permissions needed for read-only consumption.

Add to your `~/.m2/settings.xml`:

```xml
<settings>
    <servers>
        <server>
            <id>kzmlabs-snapshots</id>
            <username>YOUR_GITHUB_USERNAME</username>
            <password>YOUR_GITHUB_PAT</password>
        </server>
    </servers>
</settings>
```

The `<id>` in `settings.xml` must match the `<id>` of the `<repository>` block in your `pom.xml`.

### 3. Add the snapshot dependency

```xml
<dependency>
    <groupId>io.github.kzmlabs.flinkstatefun</groupId>
    <artifactId>statefun-sdk-java</artifactId>
    <version>3.4.0-KZM-3.4-SNAPSHOT</version>
</dependency>
```

Replace the version with whichever in-progress snapshot you want. Browse available versions: https://github.com/orgs/kzmlabs/packages?repo_name=flink-statefun

## Maintainer flow — publishing a new snapshot

1. Bump the `<revision>` in root `pom.xml` to a `-SNAPSHOT` version (e.g. `3.4.0-KZM-3.4-SNAPSHOT`)
2. Merge that bump to `release` branch
3. Tag and push:
   ```bash
   git tag snapshot-3.4.0-KZM-3.4 -m "Snapshot 3.4.0-KZM-3.4"
   git push origin snapshot-3.4.0-KZM-3.4
   ```
4. The `Deploy Snapshot` workflow runs (~5-7 min) and publishes to GitHub Packages
5. Verify upload at https://github.com/orgs/kzmlabs/packages?repo_name=flink-statefun
