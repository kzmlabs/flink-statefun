# TODO: Change GroupId Namespace

## Goal
Change from `io.github.kzmlabs` to `io.github.kzmlabs.statefun` to allow multiple projects under the kzmlabs namespace.

## Future Project Structure
```
io.github.kzmlabs.statefun      - Flink StateFun (this project)
io.github.kzmlabs.entertainment - Future project
io.github.kzmlabs.other         - Future project
```

## Steps to Implement

### 1. Update root pom.xml
```xml
<!-- Change from -->
<groupId>io.github.kzmlabs</groupId>

<!-- Change to -->
<groupId>io.github.kzmlabs.statefun</groupId>
```

### 2. Update all child module dependencies
All internal dependencies need updating:
```xml
<!-- Change from -->
<dependency>
    <groupId>io.github.kzmlabs</groupId>
    <artifactId>statefun-sdk-java</artifactId>
</dependency>

<!-- Change to -->
<dependency>
    <groupId>io.github.kzmlabs.statefun</groupId>
    <artifactId>statefun-sdk-java</artifactId>
</dependency>
```

### 3. Files to update
Run this to find all files:
```bash
grep -r "io.github.kzmlabs" --include="pom.xml" -l
```

Main files:
- `pom.xml` (root)
- All module `pom.xml` files (~30 files)
- `.github/workflows/release.yml` (update release notes)
- `RELEASE-GUIDE.md`
- `README.md`

### 4. Quick sed command (Linux/Mac)
```bash
find . -name "pom.xml" -not -path "*/target/*" -exec sed -i 's/io\.github\.kzmlabs</io.github.kzmlabs.statefun</g' {} \;
```

### 5. Version bump
After namespace change, bump to new version:
```bash
mvn versions:set -DnewVersion=3.4.0-KZM-2.0 -DgenerateBackupPoms=false
```

### 6. Test locally
```bash
mvn clean install -DskipTests
```

### 7. Release
```bash
git add -A
git commit -m "Change groupId to io.github.kzmlabs.statefun for multi-project support"
git push origin release
git tag v3.4.0-KZM-2.0
git push origin v3.4.0-KZM-2.0
```

## Breaking Change Notice

Update README and release notes:

```markdown
## Breaking Change in 3.4.0-KZM-2.0

GroupId changed from `io.github.kzmlabs` to `io.github.kzmlabs.statefun`.

Update your dependencies:
```xml
<!-- Old -->
<groupId>io.github.kzmlabs</groupId>

<!-- New -->
<groupId>io.github.kzmlabs.statefun</groupId>
```
```

## Notes
- No need to register new namespace - `io.github.kzmlabs` ownership covers all sub-namespaces
- Consider keeping old artifacts available for transition period
- Update any documentation/examples
