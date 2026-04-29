---
layout: default
title: How to IAM
nav_order: 5
parent: Usage Guides
---
# IAM (Identity and Access Management)

The `IamClient` class in the `multicloudj` library provides a cloud-agnostic interface for managing cloud identities and their access policies. It supports creating roles and service accounts, attaching and removing inline policies, and querying identity metadata across AWS and GCP.

Policies are expressed using a substrate-neutral `PolicyDocument` model that is automatically translated to provider-specific formats at runtime.

---

## Feature Support Across Providers

### Core API Features

| Feature Name | GCP | AWS | Comments |
|--------------|-----|-----|----------|
| **Create Identity** | ✅ Supported | ✅ Supported | Creates a service account (GCP) or IAM role (AWS) |
| **Delete Identity** | ✅ Supported | ✅ Supported | Deletes a service account or IAM role |
| **Get Identity** | ✅ Supported | ✅ Supported | Returns service account email (GCP) or role ARN (AWS) |
| **Attach Inline Policy** | ✅ Supported | ✅ Supported | Grants permissions to an identity |
| **Get Inline Policy Details** | ✅ Supported | ✅ Supported | Returns the policy document for a given identity |
| **Get Attached Policies** | ✅ Supported | ✅ Supported | Lists all policies attached to an identity |
| **Remove Policy** | ✅ Supported | ✅ Supported | Revokes a policy from an identity |
| **Trust Configuration** | ✅ Supported | ✅ Supported | Configures which principals can assume/use the identity |
| **ALLOW Statements** | ✅ Supported | ✅ Supported | Grant access to resources |
| **DENY Statements** | ❌ Not Supported | ✅ Supported | GCP IAM v1 is deny-by-default; DENY is silently skipped |
| **Policy Conditions** | ❌ Not Supported | ✅ Supported | GCP throws InvalidArgumentException if conditions are present |

### Configuration Options

| Configuration | GCP | AWS | Comments |
|---------------|-----|-----|----------|
| **Endpoint Override** | ✅ Supported | ✅ Supported | Custom endpoint configuration |
| **Credentials Override** | ✅ Supported | ✅ Supported | Custom credential providers via STS |
| **Create Options** | ❌ Not Applicable | ✅ Supported | Path, maxSessionDuration, permissionBoundary (AWS-specific) |

### Provider-Specific Notes

**AWS**
- IAM is a global service. The `region` parameter is normalized to `us-east-1` internally.
- Identity creation is idempotent — if the role already exists, it is updated if the description or trust policy differs.
- `tenantId` maps to the AWS account ID (12-digit number).
- `getIdentity()` returns the role ARN (`arn:aws:iam::123456789012:role/RoleName`).
- Inline policy names must be provided when attaching and retrieving policies.

**GCP**
- IAM operations require a GCP project ID as `tenantId`.
- `getIdentity()` returns the service account email (`name@project.iam.gserviceaccount.com`).
- Identity creation is idempotent — silently succeeds if the service account already exists.
- `policyName` is not used in `getInlinePolicyDetails` — GCP does not have named policies. Use `roleName` instead (must be a valid GCP role, e.g., `roles/storage.objectViewer`).
- **DENY statements are not supported** — GCP IAM v1 is deny-by-default. DENY statements in a `PolicyDocument` are silently skipped.
- **Conditions are not supported** — Attaching a policy with conditions throws `InvalidArgumentException`. GCP condition expressions require the v2 IAM API (CEL syntax).
- Trust configuration grants `roles/iam.serviceAccountTokenCreator` to the specified principals.
- Resource path prefix `projects/{projectId}` is added automatically if not already present.

---

## Creating a Client

```java
IamClient iamClient = IamClient.builder("aws")
    .withRegion("us-east-1")
    .build();
```

For GCP:

```java
IamClient iamClient = IamClient.builder("gcp")
    .withRegion("us-central1")
    .build();
```

With optional configuration:

```java
IamClient iamClient = IamClient.builder("aws")
    .withRegion("us-east-1")
    .withEndpoint(URI.create("https://custom-iam-endpoint.com"))
    .withCredentialsOverrider(credentialsOverrider)
    .build();
```

---

## Policy Model

Permissions are expressed using a substrate-neutral `PolicyDocument`. At the time a policy is attached, the SDK translates it to the provider-specific format (AWS IAM JSON or GCP role bindings).

### Actions

The SDK provides pre-defined action constants for common services:

```java
// Storage actions
StorageActions.GET_OBJECT       // s3:GetObject / roles/storage.objectViewer
StorageActions.PUT_OBJECT       // s3:PutObject / roles/storage.objectCreator
StorageActions.DELETE_OBJECT    // s3:DeleteObject / roles/storage.objectAdmin
StorageActions.LIST_BUCKET      // s3:ListBucket / roles/storage.objectViewer
StorageActions.CREATE_BUCKET    // s3:CreateBucket / roles/storage.admin
StorageActions.DELETE_BUCKET    // s3:DeleteBucket / roles/storage.admin
StorageActions.ALL              // s3:* / roles/storage.admin

// Compute actions
ComputeActions.CREATE_INSTANCE  // ec2:RunInstances / roles/compute.instanceAdmin.v1
ComputeActions.DELETE_INSTANCE  // ec2:TerminateInstances / roles/compute.instanceAdmin.v1
ComputeActions.DESCRIBE_INSTANCES // ec2:DescribeInstances / roles/compute.viewer
ComputeActions.ALL              // ec2:* / roles/compute.admin

// IAM actions
IamActions.ASSUME_ROLE          // sts:AssumeRole / roles/iam.serviceAccountUser
IamActions.CREATE_ROLE          // iam:CreateRole / roles/iam.serviceAccountAdmin
IamActions.GET_ROLE             // iam:GetRole / roles/iam.serviceAccountViewer
IamActions.ALL                  // iam:* / roles/iam.serviceAccountAdmin
```

You can also define custom actions using `Action.of("service:operation")`:

```java
Action customAction = Action.of("storage:GetObject");
Action wildcardAction = Action.wildcard("storage"); // storage:*
```

### Building a PolicyDocument

```java
Statement statement = Statement.builder()
    .effect(Effect.ALLOW)
    .actions(List.of(StorageActions.GET_OBJECT, StorageActions.LIST_BUCKET))
    .resources(List.of("my-bucket/*"))
    .build();

PolicyDocument policy = PolicyDocument.builder()
    .statements(List.of(statement))
    .build();
```

### Policy Conditions (AWS only)

Use `ConditionOperator` to add conditions to statements. Conditions are not supported on GCP.

```java
Statement statement = Statement.builder()
    .effect(Effect.ALLOW)
    .actions(List.of(StorageActions.GET_OBJECT))
    .resources(List.of("my-bucket/*"))
    .condition(ConditionOperator.STRING_EQUALS, "aws:RequestedRegion", "us-west-2")
    .build();
```

Supported condition operators include: `STRING_EQUALS`, `STRING_NOT_EQUALS`, `STRING_LIKE`, `STRING_NOT_LIKE`, `NUMERIC_EQUALS`, `NUMERIC_LESS_THAN`, `NUMERIC_GREATER_THAN`, `DATE_EQUALS`, `BOOL`, `IP_ADDRESS`, `NOT_IP_ADDRESS`, and their variants.

---

## Creating an Identity

Creates an IAM role (AWS) or service account (GCP). Returns the unique identifier of the created identity.

```java
String identityId = iamClient.createIdentity(
    "my-service-role",           // identityName
    "Role for my service",       // description
    "123456789012",              // tenantId (AWS account ID or GCP project ID)
    "us-east-1",                 // region
    Optional.empty(),            // trustConfig (see below)
    Optional.empty()             // createOptions (see below)
);
// AWS returns: arn:aws:iam::123456789012:role/my-service-role
// GCP returns: my-service-role@my-project.iam.gserviceaccount.com
```

### Trust Configuration

`TrustConfiguration` specifies which principals are allowed to assume (AWS) or impersonate (GCP) the identity.

```java
TrustConfiguration trustConfig = TrustConfiguration.builder()
    .addTrustedPrincipal("arn:aws:iam::123456789012:role/caller-role")  // AWS ARN
    .build();

// Or for GCP (service account email)
TrustConfiguration trustConfig = TrustConfiguration.builder()
    .addTrustedPrincipal("serviceAccount:caller@my-project.iam.gserviceaccount.com")
    .build();

String identityId = iamClient.createIdentity(
    "my-service-role",
    "Role for my service",
    "123456789012",
    "us-east-1",
    Optional.of(trustConfig),
    Optional.empty()
);
```

Multiple principals can be added at once:

```java
TrustConfiguration trustConfig = TrustConfiguration.builder()
    .addTrustedPrincipals(List.of(
        "arn:aws:iam::123456789012:role/role-a",
        "arn:aws:iam::123456789012:role/role-b"
    ))
    .build();
```

### Create Options (AWS only)

`CreateOptions` allows you to set AWS-specific role properties at creation time:

```java
CreateOptions options = CreateOptions.builder()
    .path("/service-roles/")
    .maxSessionDuration(7200)                                     // seconds
    .permissionBoundary("arn:aws:iam::123456789012:policy/Boundary")
    .build();

iamClient.createIdentity(
    "my-service-role",
    "Role for my service",
    "123456789012",
    "us-east-1",
    Optional.empty(),
    Optional.of(options)
);
```

---

## Attaching an Inline Policy

Attaches a `PolicyDocument` to an identity, granting the specified permissions.

```java
Statement statement = Statement.builder()
    .effect(Effect.ALLOW)
    .actions(List.of(StorageActions.GET_OBJECT, StorageActions.PUT_OBJECT))
    .resources(List.of("my-bucket/*"))
    .build();

PolicyDocument policy = PolicyDocument.builder()
    .statements(List.of(statement))
    .build();

AttachInlinePolicyRequest request = AttachInlinePolicyRequest.builder()
    .identityName("my-service-role")
    .policyDocument(policy)
    .policyName("my-storage-policy")    // required for AWS; ignored by GCP
    .tenantId("123456789012")
    .region("us-east-1")
    .build();

iamClient.attachInlinePolicy(request);
```

---

## Retrieving Policy Details

Returns the policy document attached to an identity as a JSON string.

```java
GetInlinePolicyDetailsRequest request = GetInlinePolicyDetailsRequest.builder()
    .identityName("my-service-role")
    .policyName("my-storage-policy")         // policy name (AWS); ignored by GCP
    .roleName("roles/storage.objectViewer")  // required for GCP; ignored by AWS
    .tenantId("123456789012")
    .region("us-east-1")
    .build();

String policyJson = iamClient.getInlinePolicyDetails(request);
System.out.println(policyJson);
```

---

## Listing Attached Policies

Returns the names of all policies attached to an identity.

```java
GetAttachedPoliciesRequest request = GetAttachedPoliciesRequest.builder()
    .identityName("my-service-role")
    .tenantId("123456789012")
    .region("us-east-1")
    .build();

List<String> policyNames = iamClient.getAttachedPolicies(request);
policyNames.forEach(System.out::println);
```

---

## Removing a Policy

Detaches a named policy from an identity.

```java
iamClient.removePolicy(
    "my-service-role",   // identityName
    "my-storage-policy", // policyName
    "123456789012",      // tenantId
    "us-east-1"          // region
);
```

---

## Getting Identity Metadata

Returns the unique identifier for an identity — ARN on AWS, service account email on GCP.

```java
String identityId = iamClient.getIdentity(
    "my-service-role",
    "123456789012",
    "us-east-1"
);
System.out.println(identityId);
// AWS: arn:aws:iam::123456789012:role/my-service-role
// GCP: my-service-role@my-project.iam.gserviceaccount.com
```

---

## Deleting an Identity

```java
iamClient.deleteIdentity(
    "my-service-role",
    "123456789012",
    "us-east-1"
);
```

---

## Error Handling

All operations may throw `SubstrateSdkException` subclasses:

| Exception | Cause |
|-----------|-------|
| `ResourceNotFoundException` | Identity or policy does not exist |
| `ResourceAlreadyExistsException` | Identity already exists (non-idempotent path) |
| `ResourceConflictException` | Identity has attached policies and cannot be deleted |
| `ResourceExhaustedException` | Provider limit exceeded |
| `InvalidArgumentException` | Malformed policy document or unsupported operation (e.g., conditions on GCP) |

```java
try {
    iamClient.deleteIdentity("my-service-role", "123456789012", "us-east-1");
} catch (ResourceConflictException e) {
    // Role still has attached policies — remove them first
} catch (ResourceNotFoundException e) {
    // Role does not exist
} catch (SubstrateSdkException e) {
    e.printStackTrace();
}
```

---

## Closing the Client

```java
iamClient.close();
```

Use try-with-resources for guaranteed cleanup:

```java
try (IamClient iamClient = IamClient.builder("aws").withRegion("us-east-1").build()) {
    iamClient.createIdentity("my-role", "desc", "123456789012", "us-east-1",
        Optional.empty(), Optional.empty());
}
```
