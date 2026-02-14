#!/bin/bash
#
# Release script for Kzmlabs Flink StateFun
# Usage: ./tools/release.sh <version>
# Example: ./tools/release.sh 3.4.0-KZM-1.0
#

set -e

VERSION=$1

if [ -z "$VERSION" ]; then
    echo "Usage: ./tools/release.sh <version>"
    echo "Example: ./tools/release.sh 3.4.0-KZM-1.0"
    exit 1
fi

# Validate version format (should not contain -SNAPSHOT)
if [[ "$VERSION" == *"-SNAPSHOT"* ]]; then
    echo "Error: Version should not contain -SNAPSHOT"
    exit 1
fi

echo "Releasing version: $VERSION"
echo ""

# Check for uncommitted changes
if ! git diff-index --quiet HEAD --; then
    echo "Error: You have uncommitted changes. Please commit or stash them first."
    exit 1
fi

# Make sure we're on release branch
BRANCH=$(git rev-parse --abbrev-ref HEAD)
if [ "$BRANCH" != "release" ]; then
    echo "Error: You must be on the 'release' branch. Currently on: $BRANCH"
    exit 1
fi

# Pull latest changes
echo "Pulling latest changes..."
git pull origin release

# Update Maven version
echo "Updating Maven version to $VERSION..."
mvn versions:set -DnewVersion=$VERSION -DgenerateBackupPoms=false -q

# Update Docker build script version
echo "Updating Docker build script version..."
sed -i "s/^VERSION_TAG=.*/VERSION_TAG=$VERSION/" tools/docker/build-stateful-functions.sh

# Commit the version change
echo "Committing version change..."
git add -A
git commit -m "Release $VERSION"

# Create tag
echo "Creating tag v$VERSION..."
git tag "v$VERSION"

# Push commit and tag
echo "Pushing to remote..."
git push origin release
git push origin "v$VERSION"

echo ""
echo "Release v$VERSION initiated!"
echo ""
echo "GitHub Actions will now:"
echo "  1. Build and test"
echo "  2. Deploy to Maven Central"
echo "  3. Push Docker image to ghcr.io/kzmlabs/flink-statefun:$VERSION"
echo "  4. Create GitHub Release"
echo ""
echo "Monitor progress at: https://github.com/kzmlabs/flink-statefun/actions"
echo ""

# Calculate next version (increment last number)
NEXT_VERSION=$(echo $VERSION | sed 's/\([0-9]*\)$/\1+1/' | bc)
if [ -z "$NEXT_VERSION" ]; then
    # Fallback: just append .1 if can't parse
    NEXT_VERSION="${VERSION}.1"
fi
NEXT_SNAPSHOT="${VERSION%.*}.$((${VERSION##*.}+1))-SNAPSHOT"

read -p "Prepare next development version ($NEXT_SNAPSHOT)? [Y/n] " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]] || [[ -z $REPLY ]]; then
    mvn versions:set -DnewVersion=$NEXT_SNAPSHOT -DgenerateBackupPoms=false -q
    sed -i "s/^VERSION_TAG=.*/VERSION_TAG=$NEXT_SNAPSHOT/" tools/docker/build-stateful-functions.sh
    git add -A
    git commit -m "Prepare next development version $NEXT_SNAPSHOT"
    git push origin release
    echo "Next development version $NEXT_SNAPSHOT prepared and pushed."
fi

echo ""
echo "Done!"
