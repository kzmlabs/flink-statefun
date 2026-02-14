# TODO: Change GroupId Namespace

## Goal
Change from `io.github.kzmlabs` to `io.github.kzmlabs.flinkstatefun`

No backwards compatibility needed - no clients yet.

## Steps

### 1. Update all pom.xml files
```bash
# Find and replace in all pom.xml files
find . -name "pom.xml" -not -path "*/target/*" -exec sed -i 's/io\.github\.kzmlabs</io.github.kzmlabs.flinkstatefun</g' {} \;
```

### 2. Verify changes
```bash
grep -r "io.github.kzmlabs" --include="pom.xml" | grep -v "flinkstatefun"
# Should return nothing
```

### 3. Update version
```bash
mvn versions:set -DnewVersion=3.4.0-KZM-2.0 -DgenerateBackupPoms=false
```

### 4. Test build
```bash
mvn clean install -DskipTests
```

### 5. Commit and release
```bash
git add -A
git commit -m "Change groupId to io.github.kzmlabs.flinkstatefun"
git push origin release
git tag v3.4.0-KZM-2.0
git push origin v3.4.0-KZM-2.0
```

## New Maven Coordinates
```xml
<dependency>
    <groupId>io.github.kzmlabs.flinkstatefun</groupId>
    <artifactId>statefun-sdk-java</artifactId>
    <version>3.4.0-KZM-2.0</version>
</dependency>
```

## New Docker Image
```bash
docker pull ghcr.io/kzmlabs/flink-statefun:3.4.0-KZM-2.0
```
