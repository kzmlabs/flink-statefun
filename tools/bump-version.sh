#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Bump the project version in one shot. Updates every file that references
# the current version, leaving git-staged changes for review/commit.
#
# Usage:
#   ./tools/bump-version.sh <new-version>
#
# Example:
#   ./tools/bump-version.sh 3.4.0-KZM-3.0-RC2
#
# What it touches:
#   - pom.xml                          <revision>...</revision>      (Maven source of truth)
#   - CITATION.cff                     version: "..."
#   - docs/config.toml                 Version, VersionTitle
#   - README.md                        Maven coordinate + docker pull examples
#
# After running, review with `git diff` and commit:
#   git add -u && git commit -m "Release X.Y.Z"

set -euo pipefail

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <new-version>" >&2
    echo "  e.g. $0 3.4.0-KZM-3.0-RC2" >&2
    exit 1
fi

NEW="$1"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# Extract current version from root pom.xml's <revision> property (single source of truth)
OLD=$(grep -oP '<revision>\K[^<]+' pom.xml | head -1)

if [ -z "$OLD" ]; then
    echo "ERROR: couldn't read current version from pom.xml <revision>" >&2
    exit 2
fi

if [ "$OLD" = "$NEW" ]; then
    echo "Already at $NEW; nothing to do."
    exit 0
fi

echo "Bumping: $OLD -> $NEW"
echo

update() {
    local file="$1"
    if [ ! -f "$file" ]; then
        echo "  skip   $file (missing)"
        return
    fi
    if ! grep -qF "$OLD" "$file"; then
        echo "  skip   $file (no $OLD found)"
        return
    fi
    # Use sed with a tmp file for cross-platform compatibility (Windows git-bash, macOS, Linux)
    sed -i.bak "s|$OLD|$NEW|g" "$file"
    rm -f "$file.bak"
    local count=$(grep -cF "$NEW" "$file" || true)
    echo "  update $file ($count occurrences)"
}

update "pom.xml"
update "CITATION.cff"
update "docs/config.toml"
update "README.md"

# Today's date for CITATION.cff date-released
TODAY=$(date +%Y-%m-%d)
if [ -f "CITATION.cff" ]; then
    sed -i.bak "s|^date-released:.*|date-released: \"$TODAY\"|" CITATION.cff
    rm -f "CITATION.cff.bak"
    echo "  update CITATION.cff date-released -> $TODAY"
fi

echo
echo "Done. Review with: git diff"
echo "Commit with:        git add -u && git commit -m 'Release $NEW'"
echo "Tag and push with:  git tag v$NEW && git push origin v$NEW"
