#!/bin/bash

# Script to generate JavaDoc and Jekyll HTML, then copy to site branch.
# Usage: ./scripts/generate-site.sh [version]
#
# Prerequisites:
#   - Ruby 3.3 (brew install ruby@3.3)
#   - Bundler (gem install bundler)

set -e

SITE_BRANCH="site"
UPSTREAM_REMOTE="upstream"

# 1. Fetch tags from upstream so the latest release is available locally
echo "Fetching tags from ${UPSTREAM_REMOTE}..."
git fetch "$UPSTREAM_REMOTE" --tags --prune --prune-tags

# 2. Determine the release tag to build from (arg overrides auto-detect)
if [ -n "$1" ]; then
    RELEASE_TAG="$1"
else
    RELEASE_TAG=$(git tag -l 'multicloudj-v*' --sort=-v:refname | head -n 1)
fi

if [ -z "$RELEASE_TAG" ]; then
    echo "ERROR: could not determine latest release tag (looked for multicloudj-v*)" >&2
    exit 1
fi

# Derive the numeric version (e.g. multicloudj-v0.4.0 -> 0.4.0) for output paths
VERSION="${RELEASE_TAG#multicloudj-v}"
VERSION="${VERSION#v}"

TEMP_DIR=$(mktemp -d)
TARGET_DIR="docs/api/java/${VERSION}"
LATEST_DIR="docs/api/java/latest"

echo "Building site for release tag ${RELEASE_TAG} (version ${VERSION})"

# 3. Checkout the release tag (detached HEAD) to build sources from that revision
git checkout "$RELEASE_TAG"

# 4. Generate delomboked sources and JavaDoc in temp dir
echo "Generating delomboked sources..."
mvn clean generate-sources -DskipTests

echo "Generating JavaDoc from delomboked sources..."
mvn site -DskipTests
cp -r target/site/apidocs/* "$TEMP_DIR/"

# 5. Checkout site branch
git checkout "$SITE_BRANCH"

# 6. Copy JavaDoc to versioned directory (overwrite if exists)
rm -rf "$TARGET_DIR"
mkdir -p "$TARGET_DIR"
cp -r "$TEMP_DIR"/* "$TARGET_DIR/"

# 7. Update latest symlink
rm -f "$LATEST_DIR"
ln -sf "$VERSION" "$LATEST_DIR"

# 8. Generate Jekyll HTML from Markdown sources
echo "Running Jekyll build..."
export PATH="/opt/homebrew/opt/ruby@3.3/bin:$PATH"
bundle install --quiet
bundle exec jekyll build --destination docs

# 9. Show result
echo "JavaDoc for version $VERSION copied to $TARGET_DIR (site branch)"
echo "Latest symlink updated to point to $VERSION"
echo "Jekyll HTML generated in docs/"
echo "You can now commit and push the changes."

# Cleanup
test -d "$TEMP_DIR" && rm -rf "$TEMP_DIR"
