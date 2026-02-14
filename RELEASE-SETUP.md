# Release Setup Guide

This guide covers setting up Maven Central and GitHub Container Registry publishing.

## 1. GitHub Container Registry (GHCR) - Already Done!

GHCR uses `GITHUB_TOKEN` which is automatic. No setup needed.

Image will be: `ghcr.io/kzmlabs/flink-statefun:<version>`

## 2. Maven Central (Sonatype OSSRH) Setup

### Step 1: Create Sonatype Account

1. Go to https://central.sonatype.com/
2. Click "Sign In" → "Sign up"
3. Use your GitHub account or email

### Step 2: Verify Namespace Ownership

Since your groupId is `io.github.kzmlabs`, you need to verify you own this namespace:

1. Go to https://central.sonatype.com/publishing/namespaces
2. Click "Add Namespace"
3. Enter: `io.github.kzmlabs`
4. Sonatype will ask you to verify ownership by creating a specific repo in your GitHub account
5. Create the verification repo as instructed
6. Click "Verify" - this may take a few minutes

### Step 3: Generate GPG Key

Run these commands in Git Bash:

```bash
# Generate a new GPG key
gpg --full-generate-key
# Select: (1) RSA and RSA
# Key size: 4096
# Expiration: 0 (never expires)
# Real name: Kzmlabs
# Email: your-email@example.com
# Passphrase: <choose a strong passphrase - save this!>

# List your keys to get the key ID
gpg --list-secret-keys --keyid-format=long

# Output looks like:
# sec   rsa4096/ABCD1234EFGH5678 2024-01-01 [SC]
#       FULL_KEY_FINGERPRINT_HERE
# uid                 [ultimate] Kzmlabs <your-email@example.com>
# ssb   rsa4096/XXXX1234YYYY5678 2024-01-01 [E]

# The key ID is the part after "rsa4096/" - e.g., ABCD1234EFGH5678

# Export the private key (you'll need this for GitHub secrets)
gpg --armor --export-secret-keys ABCD1234EFGH5678 > private-key.asc

# Upload public key to keyserver (required for Maven Central verification)
gpg --keyserver keyserver.ubuntu.com --send-keys ABCD1234EFGH5678
gpg --keyserver keys.openpgp.org --send-keys ABCD1234EFGH5678
```

### Step 4: Generate Sonatype Token

1. Go to https://central.sonatype.com/account
2. Click "Generate User Token"
3. Save both the **username** and **password** shown

### Step 5: Add GitHub Secrets

Go to your GitHub repo → Settings → Secrets and variables → Actions → New repository secret

Add these 4 secrets:

| Secret Name | Value |
|-------------|-------|
| `OSSRH_USERNAME` | The **username** from Sonatype token (NOT your login email) |
| `OSSRH_PASSWORD` | The **password** from Sonatype token |
| `GPG_PRIVATE_KEY` | Contents of `private-key.asc` file (entire file including headers) |
| `GPG_PASSPHRASE` | The passphrase you chose when creating the GPG key |

## 3. Making a Release

### Option A: Using the release script

```bash
./tools/release.sh 3.4.0-KZM-1.0
```

### Option B: Manual steps

```bash
# 1. Make sure you're on release branch with clean state
git checkout release
git pull origin release

# 2. Update version (remove -SNAPSHOT)
# Edit pom.xml: change 3.4.0-KZM-1.0-SNAPSHOT to 3.4.0-KZM-1.0
mvn versions:set -DnewVersion=3.4.0-KZM-1.0 -DgenerateBackupPoms=false

# 3. Update Docker build script version
# Edit tools/docker/build-stateful-functions.sh: VERSION_TAG=3.4.0-KZM-1.0

# 4. Commit the version change
git add -A
git commit -m "Release 3.4.0-KZM-1.0"

# 5. Create and push tag
git tag v3.4.0-KZM-1.0
git push origin release
git push origin v3.4.0-KZM-1.0

# 6. Prepare next development version
mvn versions:set -DnewVersion=3.4.0-KZM-1.1-SNAPSHOT -DgenerateBackupPoms=false
# Also update tools/docker/build-stateful-functions.sh
git add -A
git commit -m "Prepare next development version 3.4.0-KZM-1.1-SNAPSHOT"
git push origin release
```

## 4. Verify Release

After the GitHub Action completes:

1. **Maven Central**: Check https://central.sonatype.com/search?q=io.github.kzmlabs
   - Note: May take 10-30 minutes to appear after release

2. **Docker Image**:
   ```bash
   docker pull ghcr.io/kzmlabs/flink-statefun:3.4.0-KZM-1.0
   ```

3. **GitHub Release**: Check https://github.com/kzmlabs/flink-statefun/releases

## Troubleshooting

### GPG signing fails
- Ensure the GPG key was uploaded to keyservers
- Check that `GPG_PRIVATE_KEY` includes the full key with `-----BEGIN PGP PRIVATE KEY BLOCK-----` header

### Maven Central deployment fails
- Verify namespace is verified at https://central.sonatype.com/publishing/namespaces
- Check OSSRH credentials are from "Generate User Token", not login credentials

### Docker push fails
- GHCR should work automatically with GITHUB_TOKEN
- Check that the workflow has `packages: write` permission
