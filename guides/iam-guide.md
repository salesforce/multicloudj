---
layout: default
title: How to IAM
nav_order: 5
parent: Usage Guides
---
# IAM

The `IamClient` class in the `multicloudj` library provides a comprehensive, cloud-agnostic interface to interact with Identity and Access Management services like AWS IAM, GCP IAM, and AliCloud RAM.

This client enables creating and managing identities (roles, service accounts), attaching and managing inline policies, and configuring trust relationships across multiple cloud providers with a consistent API.

---

## Feature Support Across Providers

### Core API Features

| Feature Name | GCP              | AWS | ALI | Comments |
|--------------|------------------|-----|-----|----------|
| **Create Identity** | ✅ Supported      | ✅ Supported | 📅 In Roadmap | Create roles/service accounts with optional trust and options |
| **Get Identity** | ✅ Supported      | ✅ Supported | 📅 In Roadmap | Retrieve identity metadata (ARN, email, or roleId) |
| **Delete Identity** | ✅ Supported      | ✅ Supported | 📅 In Roadmap | Remove an identity from the cloud provider |
| **Attach Inline Policy** | ✅ Supported | ✅ Supported | 📅 In Roadmap | Attach a policy document; AWS use PutRolePolicy directly |
| **Get Attached Policies** | ✅ Supported      | ✅ Supported | 📅 In Roadmap | List inline policies attached to an identity |
| **Get Inline Policy Details** | ✅ Supported      | ✅ Supported | 📅 In Roadmap | Retrieve policy document details |
| **Remove Policy** | ✅ Supported      | ✅ Supported | 📅 In Roadmap | Remove an inline policy from an identity |

### Configuration Options

| Configuration | GCP | AWS | ALI | Comments |
|---------------|-----|-----|-----|----------|
| **Trust Configuration** | ✅ Supported | ✅ Supported | 📅 In Roadmap | Principals and conditions for assume/impersonate |

---

### Provider-Specific Notes

**AWS (IAM)**
- Tenant ID is the AWS account ID (12-digit). IAM is global per partition; region is used by the IAM client to resolve the partition and its endpoint.
- Get inline policy details: `policyName` is required.

**GCP (IAM)**
- Tenant ID: for identity operations use project ID (or `projects/...`); for policy operations use the resource that owns the IAM policy (e.g. `projects/my-project`, `folders/123`).
- Create Identity creates a Service Account on GCP. You provide the service account ID; it returns email `{id}@{project}.iam.gserviceaccount.com`. Create options are unused.
- Attach policy: `resource` is the IAM member (e.g. `serviceAccount:...`); policy actions are GCP role names (e.g. `roles/storage.objectViewer`). Get inline policy details: `roleName` is required; `policyName` is not used. 
- Remove policy: `policyName` is the role name to remove.

---

## Creating the Client

### Basic Client

```java
IamClient iamClient = IamClient.builder("aws")
    .withRegion("us-west-2")
    .build();
```

Use the appropriate provider ID: `"aws"`, `"gcp"`, or `"ali"`. The client implements `AutoCloseable`; use try-with-resources or call `close()` when done.

---

## Identity Operations

### Creating an Identity

```java
try (IamClient iamClient = IamClient.builder("aws").withRegion("us-west-2").build()) {

    String identityId = iamClient.createIdentity(
        "MyRole",
        "Example role for storage access",
        "123456789012",
        "us-west-2",
        Optional.empty(),
        Optional.empty()
    );
}
```

With trust configuration:

```java
TrustConfiguration trustConfig = TrustConfiguration.builder()
    .addTrustedPrincipal("arn:aws:iam::123456789012:root")
    .build();

String identityId = iamClient.createIdentity(
    "CrossAccountRole",
    "Role assumable by account 123456789012",
    "123456789012",
    "us-west-2",
    Optional.of(trustConfig),
    Optional.empty()
);
```

With creation options (path, max session duration, permission boundary):

```java
CreateOptions options = CreateOptions.builder()
    .path("/service-roles/")
    .maxSessionDuration(3600)
    .build();

String identityId = iamClient.createIdentity(
    "ServiceRole",
    "Role for backend service",
    "123456789012",
    "us-west-2",
    Optional.empty(),
    Optional.of(options)
);
```

### Getting Identity Metadata

```java
String identityInfo = iamClient.getIdentity("MyRole", "123456789012", "us-west-2");
```

### Deleting an Identity

```java
iamClient.deleteIdentity("MyRole", "123456789012", "us-west-2");
```

---

## Policy Operations

### Building a Policy Document

The example below uses AWS-style actions and resources (version `2012-10-17`, S3 actions, ARN resource).

```java
PolicyDocument policy = PolicyDocument.builder()
    .version("2012-10-17")
    .statement("StorageAccess")
        .effect("Allow")
        .addAction("s3:GetObject")
        .addAction("s3:PutObject")
        .addResource("arn:aws:s3:::my-bucket/*")
        .condition("StringEquals", "aws:RequestedRegion", "us-west-2")
    .endStatement()
    .build();
```

### Attaching an Inline Policy

```java
iamClient.attachInlinePolicy(policy, "123456789012", "us-west-2", "MyRole");
```

### Listing Attached Policies

```java
List<String> policyNames = iamClient.getAttachedPolicies("MyRole", "123456789012", "us-west-2");
policyNames.forEach(name -> System.out.println("Policy: " + name));
```

### Getting Inline Policy Details

```java
String policyJson = iamClient.getInlinePolicyDetails(
    "MyRole",
    "StorageAccess",
    "MyRole",
    "123456789012",
    "us-west-2"
);
```

### Removing a Policy

```java
iamClient.removePolicy("MyRole", "StorageAccess", "123456789012", "us-west-2");
```

---

**Important:** Parameter semantics (identityName, policyName, roleName, tenantId) differ by provider. See Provider-Specific Notes above.

---

## Error Handling

### Exception Handling

All IAM operations may throw `SubstrateSdkException`:

```java
try {
    String identityId = iamClient.createIdentity("MyRole", "Description", "123456789012", "us-west-2",
        Optional.empty(), Optional.empty());
} catch (SubstrateSdkException e) {
    // Handle access denied, validation errors, etc.
    e.printStackTrace();
}
```
