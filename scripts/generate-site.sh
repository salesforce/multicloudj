#!/bin/bash

# Advanced script to generate versioned JavaDoc documentation for MultiCloudJ
# Generates docs in temporary location, checks out site branch, then copies to versioned structure

set -e

# Configuration
PROJECT_NAME="multicloudj"
VERSION=${1:-$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)}
DOCS_DIR="docs"
JAVADOC_DIR="${DOCS_DIR}/api"
TEMP_DIR=$(mktemp -d)
SITE_TEMP_DIR=$(mktemp -d)
SITE_BRANCH="site"
LATEST_DIR="${JAVADOC_DIR}/latest"
VERSION_DIR="${JAVADOC_DIR}/${VERSION}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Cleanup function
cleanup() {
    if [ -d "$TEMP_DIR" ]; then
        print_info "Cleaning up temporary directory: $TEMP_DIR"
        rm -rf "$TEMP_DIR"
    fi
    if [ -d "$SITE_TEMP_DIR" ]; then
        print_info "Cleaning up site temporary directory: $SITE_TEMP_DIR"
        rm -rf "$SITE_TEMP_DIR"
    fi
}

# Set trap to cleanup on exit
trap cleanup EXIT

echo "Generating JavaDoc for MultiCloudJ version: ${VERSION}"
print_info "Temporary directory: $TEMP_DIR"
print_info "Site temporary directory: $SITE_TEMP_DIR"
print_info "Site branch: $SITE_BRANCH"

# Create directories
mkdir -p "${JAVADOC_DIR}"
mkdir -p "${VERSION_DIR}"

# Generate aggregated JavaDoc in temporary location
print_info "Generating aggregated JavaDoc..."
mvn clean javadoc:aggregate \
    -Djavadoc.failOnError=false \
    -Djavadoc.failOnWarnings=false \
    -Djavadoc.encoding=UTF-8 \
    -Djavadoc.charSet=UTF-8 \
    -Djavadoc.docencoding=UTF-8 \
    -Djavadoc.source=11 \
    -Djavadoc.windowtitle="MultiCloudJ ${VERSION} API Documentation" \
    -Djavadoc.doctitle="<h1>MultiCloudJ ${VERSION} API Documentation</h1>" \
    -Djavadoc.header="<b>MultiCloudJ</b> ${VERSION}" \
    -Djavadoc.footer="<b>MultiCloudJ</b> ${VERSION}" \
    -Djavadoc.bottom="Copyright &copy; 2024 Salesforce. All rights reserved." \
    -Djavadoc.additionalJOption="-Xdoclint:none" \
    -Djavadoc.additionalJOption="-Xdoclint:-missing" \
    -Djavadoc.additionalJOption="-Xdoclint:-accessibility" \
    -Djavadoc.additionalJOption="-Xdoclint:-html" \
    -Djavadoc.additionalJOption="-Xdoclint:-reference" \
    -Djavadoc.additionalJOption="-Xdoclint:-syntax"

# Copy generated JavaDoc to temporary location first
print_info "Copying generated JavaDoc to temporary location..."
cp -r target/reports/apidocs/* "$TEMP_DIR/"

# Verify the temporary copy
if [ ! -f "$TEMP_DIR/index.html" ]; then
    print_error "JavaDoc generation failed - index.html not found in temporary directory"
    exit 1
fi

print_success "JavaDoc generated successfully in temporary location"

# Check if we're in a git repository
if git rev-parse --git-dir > /dev/null 2>&1; then
    print_info "Git repository detected"
    
    # Check if site branch exists
    if git ls-remote --heads origin "$SITE_BRANCH" | grep -q "$SITE_BRANCH"; then
        print_info "Site branch '$SITE_BRANCH' found, checking out to temporary location..."
        
        # Checkout site branch to temporary location
        git clone --branch "$SITE_BRANCH" --single-branch . "$SITE_TEMP_DIR"
        
        # Copy the existing site structure to our docs directory
        if [ -d "$SITE_TEMP_DIR" ]; then
            print_info "Copying existing site structure from $SITE_BRANCH branch..."
            cp -r "$SITE_TEMP_DIR"/* "$DOCS_DIR/" 2>/dev/null || true
        fi
    else
        print_warning "Site branch '$SITE_BRANCH' not found, creating fresh docs structure"
        
        # Create the site branch locally
        print_info "Creating local site branch..."
        git checkout -b "$SITE_BRANCH" 2>/dev/null || git checkout "$SITE_BRANCH" 2>/dev/null || true
    fi
else
    print_warning "Not in a git repository, creating fresh docs structure"
fi

# Now copy from temporary location to version directory
print_info "Copying JavaDoc to version directory: ${VERSION_DIR}"
cp -r "$TEMP_DIR"/* "${VERSION_DIR}/"

# Create/update latest symlink
print_info "Updating latest symlink..."
if [ -L "${LATEST_DIR}" ]; then
    rm "${LATEST_DIR}"
elif [ -d "${LATEST_DIR}" ]; then
    rm -rf "${LATEST_DIR}"
fi
ln -sf "${VERSION}" "${LATEST_DIR}"

# Create version index page
print_info "Creating version index page..."
cat > "${JAVADOC_DIR}/index.html" << EOF
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MultiCloudJ API Documentation</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        .container { max-width: 800px; margin: 0 auto; }
        h1 { color: #333; }
        .version-list { margin: 20px 0; }
        .version-item { margin: 10px 0; padding: 10px; border: 1px solid #ddd; border-radius: 4px; }
        .version-item a { text-decoration: none; color: #0066cc; font-weight: bold; }
        .version-item a:hover { text-decoration: underline; }
        .latest { background-color: #f0f8ff; border-color: #0066cc; }
        .latest::before { content: "Latest: "; font-weight: bold; color: #0066cc; }
    </style>
</head>
<body>
    <div class="container">
        <h1>MultiCloudJ API Documentation</h1>
        <p>Select a version to view the API documentation:</p>
        
        <div class="version-list">
EOF

# Add latest version first
echo "            <div class=\"version-item latest\">" >> "${JAVADOC_DIR}/index.html"
echo "                <a href=\"latest/\">Latest (${VERSION})</a>" >> "${JAVADOC_DIR}/index.html"
echo "            </div>" >> "${JAVADOC_DIR}/index.html"

# Add all available versions
for version_dir in ${JAVADOC_DIR}/*/; do
    if [ -d "$version_dir" ] && [ "$(basename "$version_dir")" != "latest" ]; then
        version=$(basename "$version_dir")
        echo "            <div class=\"version-item\">" >> "${JAVADOC_DIR}/index.html"
        echo "                <a href=\"${version}/\">${version}</a>" >> "${JAVADOC_DIR}/index.html"
        echo "            </div>" >> "${JAVADOC_DIR}/index.html"
    fi
done

cat >> "${JAVADOC_DIR}/index.html" << EOF
        </div>
        
        <p><a href="../">← Back to MultiCloudJ Documentation</a></p>
    </div>
</body>
</html>
EOF

# Now checkout site branch and copy the updated docs
if git rev-parse --git-dir > /dev/null 2>&1; then
    print_info "Checking out site branch and copying updated documentation..."
    
    # Checkout site branch
    git checkout "$SITE_BRANCH" 2>/dev/null || {
        print_warning "Could not checkout site branch, creating it..."
        git checkout -b "$SITE_BRANCH"
    }
    
    # Copy the entire docs directory to the site branch
    print_info "Copying updated documentation to site branch..."
    cp -r "$DOCS_DIR"/* . 2>/dev/null || true
    
    # Show git status
    print_info "Git status:"
    git status --porcelain | head -10
    
    print_success "Documentation copied to site branch '$SITE_BRANCH'"
    print_info "You can now commit and push the changes:"
    print_info "  git add ."
    print_info "  git commit -m 'docs: update JavaDoc for version ${VERSION}'"
    print_info "  git push origin $SITE_BRANCH"
fi

# Show summary of what was created
print_success "JavaDoc generation completed successfully!"
echo ""
echo "📁 Directory Structure Created:"
echo "  ├── ${DOCS_DIR}/"
echo "  │   ├── api/"
echo "  │   │   ├── index.html          # Version selection page"
echo "  │   │   ├── latest/             # Symlink to latest version"
echo "  │   │   └── ${VERSION}/         # Version ${VERSION} documentation"
echo "  │   │       ├── index.html"
echo "  │   │       ├── allclasses-index.html"
echo "  │   │       ├── allpackages-index.html"
echo "  │   │       └── ..."
echo "  │   └── ... (existing docs)"
echo ""
echo "🔗 Access URLs:"
echo "  - Latest: ${LATEST_DIR}/"
echo "  - Version ${VERSION}: ${VERSION_DIR}/"
echo "  - Index: ${JAVADOC_DIR}/index.html"
echo ""
echo "🌿 Git Branch:"
echo "  - Current branch: $(git branch --show-current 2>/dev/null || echo 'unknown')"
echo "  - Site branch: $SITE_BRANCH"
echo ""
echo "🧹 Temporary directories cleaned up:"
echo "  - $TEMP_DIR"
echo "  - $SITE_TEMP_DIR" 
