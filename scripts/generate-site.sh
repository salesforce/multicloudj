#!/bin/bash

# Simple script to generate JavaDoc and copy to site branch in versioned directory
# Usage: ./scripts/generate-site.sh [version]

set -e

VERSION=${1:-$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)}
TEMP_DIR=$(mktemp -d)
SITE_BRANCH="site"
MAIN_BRANCH="main"
TARGET_DIR="docs/api/java/${VERSION}"
LATEST_DIR="docs/api/java/latest"

# 1. Checkout main branch
git checkout "$MAIN_BRANCH"

# 2. Generate delomboked sources and JavaDoc in temp dir
echo "Generating delomboked sources..."
mvn clean generate-sources -DskipTests

echo "Generating JavaDoc from delomboked sources..."
mvn site -DskipTests
cp -r target/site/apidocs/* "$TEMP_DIR/"

# 3. Checkout site branch
git checkout "$SITE_BRANCH"

# 4. Copy JavaDoc to versioned directory (overwrite if exists)
rm -rf "$TARGET_DIR"
mkdir -p "$TARGET_DIR"
cp -r "$TEMP_DIR"/* "$TARGET_DIR/"

# 5. Update latest symlink
rm -f "$LATEST_DIR"
ln -sf "$VERSION" "$LATEST_DIR"

# 6. Show result
echo "JavaDoc for version $VERSION copied to $TARGET_DIR (site branch)"
echo "Latest symlink updated to point to $VERSION"
echo "You can now commit and push the changes."

# Cleanup
test -d "$TEMP_DIR" && rm -rf "$TEMP_DIR" 
