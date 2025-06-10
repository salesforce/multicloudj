# Maven Central Publishing Setup Guide

This guide walks you through setting up Maven Central publishing for the MultiCloudJ project using the new [central.sonatype.com](https://central.sonatype.com) portal.

## Prerequisites

You've already completed the namespace verification for `com.salesforce.multicloudj` on central.sonatype.com.

## 1. Generate Authentication Token

1. Log in to [central.sonatype.com](https://central.sonatype.com)
2. Go to "View Account" → "Generate User Token"
3. Copy the username and password tokens that are generated
4. Keep these tokens secure - they're your authentication credentials

## 2. Configure Maven Settings

Create or update your `~/.m2/settings.xml` file:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0 
          https://maven.apache.org/xsd/settings-1.0.0.xsd">
  
  <servers>
    <server>
      <id>central</id>
      <username>YOUR_TOKEN_USERNAME</username>
      <password>YOUR_TOKEN_PASSWORD</password>
    </server>
  </servers>
  
  <profiles>
    <profile>
      <id>release</id>
      <properties>
        <gpg.keyname>YOUR_GPG_KEY_ID</gpg.keyname>
        <!-- Only include passphrase if your GPG key has one -->
        <!-- <gpg.passphrase>YOUR_GPG_PASSPHRASE</gpg.passphrase> -->
      </properties>
    </profile>
  </profiles>
  
</settings>
```

Replace:
- `YOUR_TOKEN_USERNAME` with the username token from Central Portal
- `YOUR_TOKEN_PASSWORD` with the password token from Central Portal
- `YOUR_GPG_KEY_ID` with your GPG key ID
- Uncomment and set `YOUR_GPG_PASSPHRASE` only if your key has a passphrase

### If you don't know or don't have a GPG passphrase:

**Option 1: Test if your key needs a passphrase**
```bash
# This will prompt for passphrase if needed
echo "test" | gpg --clearsign --local-user YOUR_GPG_KEY_ID
```

**Option 2: No passphrase needed**
- If your GPG key doesn't have a passphrase, simply leave that line commented out
- Maven will use your GPG key without prompting for a passphrase

**Option 3: Create a new GPG key without passphrase**
```bash
# Generate a key and when prompted for passphrase, just press Enter (no passphrase)
gpg --gen-key
```

## 3. Setup GPG Signing

### Install GPG (if not already installed):
```bash
# macOS
brew install gnupg

# Linux
sudo apt-get install gnupg
```

### Generate a GPG key (if you don't have one):
```bash
gpg --gen-key
```

### Export your public key:
```bash
gpg --keyserver keyserver.ubuntu.com --send-keys YOUR_KEY_ID
gpg --keyserver keys.openpgp.org --send-keys YOUR_KEY_ID
```

### Find your key ID:
```bash
gpg --list-secret-keys --keyid-format LONG
```

## 4. Publishing Commands

### For Snapshot Releases:
```bash
mvn clean deploy -P release
```

### For Production Releases:

1. **Update version to release version:**
   ```bash
   mvn versions:set -DnewVersion=0.1.0
   mvn versions:commit
   ```

2. **Deploy to Central Portal:**
   ```bash
   mvn clean deploy -P release
   ```

3. **Log in to Central Portal to publish:**
   - Go to [central.sonatype.com](https://central.sonatype.com)
   - Navigate to "Deployments"
   - Find your deployment and click "Publish"
   - Or use auto-publish by setting `<autoPublish>true</autoPublish>` in the plugin config

4. **Tag the release:**
   ```bash
   git tag v0.1.0
   git push origin v0.1.0
   ```

5. **Update to next snapshot version:**
   ```bash
   mvn versions:set -DnewVersion=0.2.0-SNAPSHOT
   mvn versions:commit
   ```

## 5. Verification

After publishing, your artifacts will be available at:
- `https://central.sonatype.com/artifact/com.salesforce.multicloudj/multicloudj-parent`
- `https://repo1.maven.org/maven2/com/salesforce/multicloudj/`

## 6. Common Issues and Solutions

### Issue: GPG signing fails
**Solution:** Make sure GPG is in your PATH and the key is properly configured:
```bash
gpg --list-secret-keys
export GPG_TTY=$(tty)
```

### Issue: Authentication fails
**Solution:** Verify your tokens are correct in `~/.m2/settings.xml` and haven't expired.

### Issue: Javadoc generation fails
**Solution:** The configuration includes `<doclint>none</doclint>` to be more permissive with Javadoc warnings.

## 7. Maven Central Requirements Checklist

✅ **Correct coordinates:** Using `com.salesforce.multicloudj` groupId  
✅ **Project metadata:** Name, description, URL, licenses, developers, SCM  
✅ **JAR files:** Sources and Javadoc JARs are attached  
✅ **GPG signatures:** All files are signed  
✅ **Namespace verification:** Already completed on Central Portal  

## 8. Additional Resources

- [Central Portal Documentation](https://central.sonatype.org/publish/publish-portal-maven/)
- [Maven Central Requirements](https://central.sonatype.org/publish/requirements/)
- [Central Publishing Plugin](https://central.sonatype.org/publish/publish-portal-maven/)

The latest version (0.7.0) includes support for snapshots and other improvements. 