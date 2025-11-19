# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

MultiCloudJ is a cloud-agnostic Java SDK providing unified interfaces for cloud services across AWS, GCP, and Alibaba Cloud. The SDK uses Java Service Provider Interface (SPI) pattern to load provider-specific implementations at runtime.

## Architecture

### Module Structure

The project follows a hierarchical module organization:

**Common Modules:**
- `multicloudj-common`: Core abstractions, exceptions, and utilities shared across all services
- `multicloudj-common-aws`: AWS-specific common utilities and abstractions
- `multicloudj-common-gcp`: GCP-specific common utilities and abstractions
- `multicloudj-common-ali`: Alibaba Cloud-specific common utilities and abstractions

**Service Modules:** Each service has a `-client` module (cloud-agnostic API) and provider-specific implementation modules:
- **Blob Storage**: `blob/blob-client` (API), `blob/blob-aws`, `blob/blob-gcp`, `blob/blob-ali`
- **Document Store**: `docstore/docstore-client` (API), `docstore/docstore-aws`, `docstore/docstore-gcp-firestore`, `docstore/docstore-gcp-spanner`, `docstore/docstore-ali`
- **STS (Security Token Service)**: `sts/sts-client` (API), `sts/sts-aws`, `sts/sts-gcp`, `sts/sts-ali`
- **PubSub**: `pubsub/pubsub-client` (API), `pubsub/pubsub-aws`, `pubsub/pubsub-gcp`

**Other Modules:**
- `examples`: Example code demonstrating SDK usage
- `coverage-report`: Aggregated code coverage reports

### Provider Discovery Pattern

The SDK uses Java ServiceLoader for provider discovery. Provider-specific implementations are loaded dynamically based on `providerId` (e.g., "aws", "gcp", "ali"). All provider implementations:
1. Extend abstract classes from `-client` modules (e.g., `AbstractBlobStore`, `AbstractBlobClient`)
2. Implement the `Provider` interface with `getProviderId()` method
3. Are registered in `META-INF/services/` files for ServiceLoader

### Client Architecture

Services expose two types of clients:
- **Service-level clients** (e.g., `BlobClient`): Service-wide operations like listing buckets
- **Resource-level clients** (e.g., `BucketClient`, `TopicClient`): Operations on specific resources

Clients are instantiated using builder pattern: `BlobClient.builder("aws").withRegion("us-west-2").build()`

## Common Commands


### Building and Testing

```bash
# Full build with all unit tests
a. mvn clean install

# Build without tests, before running any tests, always run this step.
b. mvn clean install -DskipTests

# Run unit tests only
c. mvn clean test

# Run the full test suite including unit and integration tests
d. mvn clean verify

# Run tests for specific module, but make sure you run command b. before this
e. mvn test -pl blob/blob-aws

# Run specific test class, but make sure you run command b. before this
f. mvn test -pl blob/blob-aws -Dtest=AwsBlobStoreTest

# Run integration tests with recording mode (updates test fixtures)
g. mvn test -pl blob/blob-aws -Dtest=AwsBlobStoreIT -Drecord

```

## Testing Conventions

### Test Types

- **Unit tests** (`*Test.java`): Mock-based tests, no external dependencies
- **Conformance tests** (`AbstractBlobStoreIT`, etc.): Base test suites in `-client` modules that provider implementations extend to ensure API compliance
  - Conformance tests are also integration tests. Conformance tests are written once on Abstract class such as `AbstractBlobStoreIT` and executed by each cloud such as `AwsBlobStoreIT`.
  - We never run the conformance tests through abstract class, only the implementation gets executed such as `AwsBlobStoreIT`
  - Conformance tests are executed in 2 modes:
    - record mode: with `-Drecord` and the credentials are supplied. This mode uses wiremock as a forward proxy and record all the http transactions as request/response
    - replay mode: default mode with no credentials required. This mode uses wiremock as a forward proxy and replays the previously recorded responses by record mode.

### Special instructions for Running Conformance Tests in record mode

Conformance tests require valid cloud credentials:
- AWS: Set `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, if they are not available, ask the user.
- GCP: Set `GOOGLE_APPLICATION_CREDENTIALS` pointing to service account JSON,  if they are not available, ask the user.
- Alibaba: It depends upon the cloud service, for tablestore it's:
  - `TABLESTORE_ACCESS_KEY_ID`, `TABLESTORE_ACCESS_KEY_SECRET`, `TABLESTORE_SESSION_TOKEN`

## Development Guidelines

### Dependency management
- Never add cloud specific dependency in cloud-agnostic package.
  - for example, `docstore-client` should never depend upon `multicloud-common-gcp`, `docstore-aws` etc

### Adding New Functionality

1. Define cloud-agnostic API in `-client` for end user, for example `BucketClient`, `DocstoreClient`
2. Define cloud-agnostic API in `-client` module's abstract class in driver such as `AbstractBlobStore`, `AbstractDocstore`
3. Implement functionality in each provider-specific module (aws, gcp, ali)
4. Add unit tests to provider modules
5. Add/update conformance tests in `-client` module
6. Ensure checkstyle passes (Google Java Style Guide)

### Working with Multiple Cloud Providers

Features must be implementable across all supported cloud providers. If a feature is provider-specific, discuss in an issue before implementing. The SDK prioritizes cross-cloud compatibility over provider-specific capabilities.


### Git operations

Look for the local instructions in ~/.claude/config.md for git push to take the auth issues

## Build Configuration

- **Java Version**: Requires Java 11+, targets Java 11 bytecode
- **Build Tool**: Maven 3.8+
- **Parent POM**: `multicloudj-parent` in root `pom.xml`
- **Version Management**: Uses `${revision}` property with flatten-maven-plugin
- **Code Coverage**: JaCoCo plugin enabled, reports in `coverage-report` module
