# Release Process

This document describes how to perform a release of MultiCloudJ to Maven Central using GitHub Actions.

## Prerequisites

### 1. Sonatype Account Setup

1. Create an account at [Sonatype Central Portal](https://central.sonatype.com/)
2. Generate a User Token:
   - Go to Account → Generate User Token
   - Save the username and token securely

### 2. GPG Key Setup

Generate a GPG key for signing artifacts:

```bash
# Generate a new GPG key (if you don't have one)
gpg --full-generate-key

# List your GPG keys to get the key ID
gpg --list-secret-keys --keyid-format LONG

# Export your private key (replace KEY_ID with your actual key ID)
gpg --armor --export-secret-keys KEY_ID > private-key.asc

# Publish your public key to a key server
gpg --keyserver keyserver.ubuntu.com --send-keys KEY_ID
```

### 3. Configure GitHub Secrets

Add the following secrets to your GitHub repository (Settings → Secrets and variables → Actions):

| Secret Name | Description | Example/Format |
|------------|-------------|----------------|
| `SONATYPE_USERNAME` | Username from Sonatype token | Generated from Sonatype portal |
| `SONATYPE_TOKEN` | Password/token from Sonatype | Generated from Sonatype portal |
| `GPG_PRIVATE_KEY` | Your GPG private key | Content of `private-key.asc` |
| `GPG_PASSPHRASE` | Passphrase for your GPG key | The passphrase you set when creating the key |

To add secrets:
1. Go to your GitHub repository
2. Click Settings → Secrets and variables → Actions
3. Click "New repository secret"
4. Add each secret with its corresponding value

## Performing a Release

### Step 1: Prepare for Release

1. Ensure all changes are merged to the `main` branch
2. Verify all tests pass: `mvn clean verify`
3. Review and update CHANGELOG.md if needed
4. Decide on the release version number (e.g., `0.2.5`)
5. Decide on the next development version (e.g., `0.2.6-SNAPSHOT`)

### Step 2: Trigger Release Workflow

1. Go to the **Actions** tab in your GitHub repository
2. Select **Maven Release** workflow from the left sidebar
3. Click **Run workflow** button
4. Fill in the inputs:
   - **Release version**: e.g., `0.2.5` (without 'v' prefix)
   - **Next development version**: e.g., `0.2.6-SNAPSHOT`
5. Click **Run workflow**

### Step 3: Monitor the Release

The workflow will:
1. ✅ Checkout code
2. ✅ Set up JDK 11 and configure Maven
3. ✅ Update version to release version
4. ✅ Build and test the project
5. ✅ Deploy artifacts to Maven Central (with GPG signing)
6. ✅ Create a Git tag (e.g., `v0.2.5`)
7. ✅ Generate changelog from commits
8. ✅ Create a GitHub Release with release notes
9. ✅ Update to next development version
10. ✅ Push changes back to `main` branch

### Step 4: Verify Release

1. Check the [Sonatype Central Portal](https://central.sonatype.com/publishing/deployments) for your deployment
2. Wait for artifacts to sync to Maven Central (can take 15-30 minutes)
3. Verify the GitHub release at: `https://github.com/salesforce/multicloudj/releases/tag/vX.Y.Z`
4. Verify artifacts are available on Maven Central:
   ```
   https://central.sonatype.com/artifact/com.salesforce.multicloudj/multicloudj-parent/X.Y.Z
   ```

## Troubleshooting

### GPG Signing Issues

If you encounter GPG signing errors:

```bash
# Verify your GPG key is properly formatted
cat private-key.asc

# Make sure it starts with: -----BEGIN PGP PRIVATE KEY BLOCK-----
# and ends with: -----END PGP PRIVATE KEY BLOCK-----
```

### Maven Central Deployment Issues

- Check that your `SONATYPE_USERNAME` and `SONATYPE_TOKEN` are correct
- Verify the token has not expired
- Check the Sonatype Central Portal for validation errors

### GitHub Release Issues

- Ensure the `GITHUB_TOKEN` has write permissions
- Verify the tag doesn't already exist

## Manual Release (Fallback)

If you need to perform a release manually:

```bash
# Set the release version
mvn versions:set -DnewVersion=0.2.5 -DgenerateBackupPoms=false

# Build and deploy
mvn clean deploy -Prelease

# Create and push tag
git tag -a v0.2.5 -m "Release version 0.2.5"
git push origin v0.2.5

# Update to next development version
mvn versions:set -DnewVersion=0.2.6-SNAPSHOT -DgenerateBackupPoms=false
git add .
git commit -m "chore: prepare next development iteration 0.2.6-SNAPSHOT"
git push origin main
```

## Release Checklist

- [ ] All tests passing on `main` branch
- [ ] CHANGELOG.md updated (optional)
- [ ] GitHub secrets configured
- [ ] Release workflow triggered with correct versions
- [ ] Release workflow completed successfully
- [ ] GitHub release created
- [ ] Artifacts visible on Maven Central
- [ ] Next development version committed to `main`
- [ ] Release announcement posted (if applicable)

## Versioning Strategy

MultiCloudJ follows [Semantic Versioning](https://semver.org/):

- **MAJOR**: Incompatible API changes
- **MINOR**: New functionality in a backwards-compatible manner
- **PATCH**: Backwards-compatible bug fixes

Development versions use the `-SNAPSHOT` suffix (e.g., `0.2.6-SNAPSHOT`).
