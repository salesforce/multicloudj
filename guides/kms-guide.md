---
layout: default
title: How to KMS
nav_order: 8
parent: Usage Guides
---
# KMS

The `KmsClient` class in the `multicloudj` library provides a cloud-agnostic interface to key management services like AWS KMS, GCP Cloud KMS, and Alibaba Cloud KMS.

This client enables creating symmetric keys, managing their lifecycle state, importing your own key material (BYOK), attaching portable access policies, tagging keys, and encrypting or decrypting small payloads, all through one consistent API.

---

## Feature Support Across Providers

### Core API Features

| Feature Name | GCP | AWS | ALI | Comments |
|--------------|-----|-----|-----|----------|
| **Create Key** | ✅ Supported | ✅ Supported | ✅ Supported | Symmetric encrypt/decrypt keys, with optional description, tags and initial policy |
| **Enable Key** | ✅ Supported | ✅ Supported | ✅ Supported | Enables a key, or a single key version where the provider tracks state per version |
| **Disable Key** | ✅ Supported | ✅ Supported | ✅ Supported | Disables a key, or a single key version where the provider tracks state per version |
| **Tag Key** | ✅ Supported | ✅ Supported | ✅ Supported | Adds or overwrites tags; existing tags with other names are kept |
| **Encrypt** | ✅ Supported | ✅ Supported | ✅ Supported | Encrypts a small payload directly with a key |
| **Decrypt** | ✅ Supported | ✅ Supported | ✅ Supported | Decrypts a ciphertext produced by `encrypt` |

### Key Policy Features

| Feature Name | GCP | AWS | ALI | Comments |
|--------------|-----|-----|-----|----------|
| **Set Key Policy** | ✅ Supported | ✅ Supported | ✅ Supported | Replaces the key's access policy using the portable `KeyPolicy` model |
| **Get Key Policy** | ✅ Supported | ✅ Supported | ✅ Supported | Native entries the portable model cannot express are omitted |
| **Deny Grants** | ❌ Not supported | ✅ Supported | ✅ Supported | Check `getPolicyCapabilities().isSupportsDeny()` |
| **Encryption Context Conditions** | ✅ Supported | ✅ Supported | ✅ Supported | Only valid on grants for cryptographic actions |
| **Other Condition Attributes** | ❌ Not supported | ✅ Supported | ❌ Not supported | `PRINCIPAL_NAME`, `SOURCE_RESOURCE`, `SOURCE_ACCOUNT`, `NATIVE` |

### Import Key Material (BYOK)

| Feature Name | GCP | AWS | ALI | Comments |
|--------------|-----|-----|-----|----------|
| **External-Origin Keys** | ✅ Supported | ✅ Supported | ✅ Supported | Create with `origin(KeyOrigin.EXTERNAL)` |
| **Get Import Parameters** | ✅ Supported | ✅ Supported | ✅ Supported | Returns the wrapping public key and an import token |
| **Import Key Material** | ✅ Supported | ✅ Supported | ✅ Supported | Imports wrapped material into an external-origin key |
| **`RSAES_OAEP_SHA_256` wrapping** | ✅ Supported | ✅ Supported | ✅ Supported | Default wrapping algorithm |
| **`RSA_AES_KEY_WRAP_SHA_256` wrapping** | ✅ Supported | ✅ Supported | ❌ Not supported | RSA-wrapped ephemeral AES key (RFC 5649) |
| **Wrapping key sizes** | RSA_3072 (default), RSA_4096 | RSA_2048, RSA_3072, RSA_4096 (default) | RSA_2048 only | Unsupported sizes throw `UnSupportedOperationException` |
| **Expiring Key Material** | ❌ Not supported | ✅ Supported | ✅ Supported | `ExpirationModel.KEY_MATERIAL_EXPIRES` with `validTo` |

### Advanced Features

| Feature Name | GCP | AWS | ALI | Comments |
|--------------|-----|-----|-----|----------|
| **Multi-Region Keys** | ✅ Supported | ✅ Supported | ❌ Not supported | GCP needs a key container in a multi-region, dual-region or `global` location. Check `supportsMultiRegionKeys()` first |
| **HSM Protection Level** | ✅ Supported | ❌ Not applied | ✅ Supported | AWS does not use `protectionLevel` |
| **Key Versions** | ✅ Supported | N/A | ✅ Supported | `KeyRef.versionId` is ignored by providers whose keys have a single state |

### Configuration Options

| Configuration | GCP | AWS | ALI | Comments |
|---------------|-----|-----|-----|----------|
| **Region** | ✅ Supported | ✅ Supported | ✅ Supported | Alibaba requires either a region or an endpoint |
| **Key Container** | ✅ Required for create | ❌ Ignored | ✅ Optional | Parent resource that keys are created in. See Provider-Specific Notes |
| **Endpoint Override** | ✅ Supported | ✅ Supported | ✅ Supported | Custom endpoint configuration |
| **Proxy Endpoint** | ✅ Supported | ✅ Supported | ✅ Supported | Send requests through a proxy |
| **Credentials Override** | ✅ Supported | ✅ Supported | ✅ Supported | Custom credentials via STS. Alibaba accepts `SESSION` credentials only |
| **Retry Config** | ✅ Supported | ✅ Supported | ✅ Supported | Alibaba applies `attemptTimeout` as the connect/read timeout |
| **Provider Properties** | ✅ Supported | ✅ Supported | ✅ Supported | Provider-specific settings that have no portable builder method |

### Provider IDs

| Provider | Provider ID |
|----------|-------------|
| AWS KMS | `aws` |
| GCP Cloud KMS | `gcp` |
| Alibaba Cloud KMS | `ali` |

---

### Provider-Specific Notes

**AWS KMS**
- `keyName` becomes an alias (`alias/<keyName>`) on the new key. The key container is ignored.
- Supports every `KeyAction`, `PrincipalType` and `ConditionAttribute`, as well as deny grants.

**GCP Cloud KMS**
- The key container is the key ring's full name, for example `projects/<project>/locations/<location>/keyRings/<ring>`. You need it to create keys and to resolve short key ids.
- `keyName` is required and becomes the crypto key id. Tags are stored as crypto key labels.
- The encryption context is sent as additional authenticated data: a JSON object with its keys sorted. You must pass the same context to `decrypt`.
- Policies support only `IDENTITY` principals, `ENCRYPTION_CONTEXT` conditions, and the actions `ENCRYPT`, `DECRYPT`, `DESCRIBE`, `MANAGE`, `IMPORT_KEY_MATERIAL` and `ALL`. Deny grants are not supported.
- Enable and disable act on a single version when `KeyRef.versionId` is set, and on the whole key otherwise.

**Alibaba Cloud KMS**
- `keyName` becomes an alias (`alias/<keyName>`). The key container, if you set one, is the dedicated KMS instance id.
- Policies support `IDENTITY` and `ACCOUNT` principals, `ENCRYPTION_CONTEXT` conditions, and deny grants. The actions are `ENCRYPT`, `DECRYPT`, `GENERATE_DATA_KEY`, `REENCRYPT`, `DESCRIBE`, `MANAGE`, `MANAGE_STATE`, `MANAGE_POLICY`, `MANAGE_TAGS`, `IMPORT_KEY_MATERIAL` and `ALL`.
- Key import supports only `RSAES_OAEP_SHA_256` with an `RSA_2048` wrapping key.

> Call `getPolicyCapabilities()` before you build a policy, so the same code can adapt to each provider. `setKeyPolicy` throws `UnSupportedOperationException` if a policy uses anything outside those capabilities.

---

## Creating the Client

### Basic Client

Take the provider, region, and key container from configuration, so the same code runs against any cloud:

```java
String providerId = config.get("kms.provider");          // e.g. "aws", "gcp", "ali"
String region = config.get("kms.region");
String keyContainer = config.get("kms.keyContainer");    // may be empty where unused

KmsClient kmsClient = KmsClient.builder(providerId)
    .withRegion(region)
    .withKeyContainer(keyContainer)
    .build();
```

The client implements `AutoCloseable`. Use try-with-resources, or call `close()` when you're done.

### Advanced Configuration

```java
KmsClient kmsClient = KmsClient.builder(providerId)
    .withRegion(region)
    .withKeyContainer(keyContainer)
    .withEndpoint(URI.create(config.get("kms.endpoint")))
    .withProxyEndpoint(URI.create(config.get("kms.proxy")))
    .withCredentialsOverrider(credentialsOverrider)
    .withRetryConfig(retryConfig)
    .build();
```

---

## Key Operations

### Creating a Key

```java
try (KmsClient kmsClient = KmsClient.builder(providerId)
        .withRegion(region)
        .withKeyContainer(keyContainer)
        .build()) {

    CreateKeyResponse created = kmsClient.createKey(CreateKeyRequest.builder()
        .keyName("orders-root-key")
        .description("Root key for order data")
        .tag("team", "orders")
        .build());

    KeyRef keyRef = created.getKeyRef();
    System.out.println("Created key " + keyRef.getKeyId() + " in state " + created.getKeyState());
}
```

`CreateKeyRequest` defaults to `KeySpec.SYMMETRIC_DEFAULT`, `KeyUsage.ENCRYPT_DECRYPT`, `KeyOrigin.KMS` and `ProtectionLevel.SOFTWARE`. To request hardware-backed keys where the provider supports them, set `.protectionLevel(ProtectionLevel.HSM)`.

### Multi-Region Keys

```java
if (kmsClient.supportsMultiRegionKeys()) {
    kmsClient.createKey(CreateKeyRequest.builder()
        .keyName("global-key")
        .multiRegion(true)
        .build());
}
```

If you set `multiRegion(true)` on a client whose configuration can't support it, `createKey` throws `UnSupportedOperationException`.

### Referencing an Existing Key

A `KeyRef` returned by the SDK can be passed straight back to any operation. To build one yourself, only `keyId` is required. It can be a short id, which is resolved against the client's region and key container, or a fully qualified native resource name:

```java
KeyRef keyRef = KeyRef.builder()
    .keyId(config.get("kms.keyId"))
    .build();
```

### Enabling and Disabling a Key

```java
kmsClient.disableKey(keyRef);
kmsClient.enableKey(keyRef);
```

If the provider versions keys, set `versionId` on the `KeyRef` to change the state of just that version.

### Tagging a Key

```java
kmsClient.tagKey(keyRef, Map.of("env", "prod", "cost-center", "1234"));
```

---

## Encrypting and Decrypting

`encrypt` and `decrypt` are meant for small payloads such as data keys or secrets. You can bind a ciphertext to an encryption context, and decryption then succeeds only when the same context is provided again.

```java
EncryptResponse encrypted = kmsClient.encrypt(EncryptRequest.builder()
    .keyRef(keyRef)
    .plaintext("secret-value".getBytes(StandardCharsets.UTF_8))
    .encryptionContextEntry("tenant", "acme")
    .build());

DecryptResponse decrypted = kmsClient.decrypt(DecryptRequest.builder()
    .keyRef(keyRef)
    .ciphertext(encrypted.getCiphertext())
    .encryptionContextEntry("tenant", "acme")
    .build());

String plaintext = new String(decrypted.getPlaintext(), StandardCharsets.UTF_8);
```

---

## Key Policies

### Building a Policy

A `KeyPolicy` is a list of `KeyGrant`s. Each grant names one or more principals, a set of portable `KeyAction`s, an `Effect` (default `ALLOW`), and optional conditions.

```java
KeyPolicy policy = KeyPolicy.builder()
    .grant(KeyGrant.builder()
        .id("admins")
        .principal(KeyPrincipal.of(PrincipalType.IDENTITY, adminIdentity))
        .action(KeyAction.MANAGE)
        .build())
    .grant(KeyGrant.builder()
        .id("app-crypto")
        .principal(KeyPrincipal.of(PrincipalType.IDENTITY, appIdentity))
        .action(KeyAction.ENCRYPT)
        .action(KeyAction.DECRYPT)
        .condition(KeyCondition.builder()
            .operator(ConditionOperator.STRING_EQUALS)
            .attribute(ConditionAttribute.ENCRYPTION_CONTEXT)
            .key("tenant")
            .value("acme")
            .build())
        .build())
    .build();
```

An `IDENTITY` principal's identifier is the identity's native identifier, exactly as the IAM client's `getIdentity` returns it.

### Checking Provider Capabilities

```java
PolicyCapabilities caps = kmsClient.getPolicyCapabilities();
if (caps.isSupportsDeny()) {
    // add a DENY grant
}
if (caps.getSupportedPrincipalTypes().contains(PrincipalType.ACCOUNT)) {
    // grant to a whole account/project
}
```

### Setting and Reading a Policy

```java
kmsClient.setKeyPolicy(keyRef, policy);

KeyPolicy current = kmsClient.getKeyPolicy(keyRef);
current.getGrants().forEach(g -> System.out.println(g.getId() + " -> " + g.getActions()));
```

You can also attach a policy when you create a key, with `CreateKeyRequest.builder().policy(policy)`.

---

## Importing Key Material (BYOK)

1. Create a key with external origin.
2. Fetch the import parameters: a wrapping public key and an import token.
3. Wrap your key material with that public key, using the returned algorithm.
4. Import the wrapped material.

```java
CreateKeyResponse created = kmsClient.createKey(CreateKeyRequest.builder()
    .keyName("byok-key")
    .origin(KeyOrigin.EXTERNAL)
    .build());

ImportParameters params = kmsClient.getImportParameters(GetImportParametersRequest.builder()
    .keyRef(created.getKeyRef())
    .wrappingAlgorithm(WrappingAlgorithm.RSAES_OAEP_SHA_256)
    .build());

byte[] wrapped = wrapKeyMaterial(params.getWrappingPublicKey(), params.getWrappingAlgorithm(), keyMaterial);

KeyRef imported = kmsClient.importKeyMaterial(ImportKeyMaterialRequest.builder()
    .keyRef(created.getKeyRef())
    .importToken(params.getImportToken())
    .wrappedKeyMaterial(wrapped)
    .build());
```

`wrappingKeySpec` is optional. If you leave it out, each provider uses its default size; see the Import Key Material table above. If the provider versions keys, the returned `KeyRef` includes the version that the import created. To make imported material expire, where the provider supports it, set `.expirationModel(ExpirationModel.KEY_MATERIAL_EXPIRES)` and `.validTo(instant)`.

---

## Error Handling

All KMS operations throw a `SubstrateSdkException` subclass. Native provider errors are mapped to these portable types:

| Exception | Typical cause |
|-----------|---------------|
| `ResourceNotFoundException` | Key, key version, or key container does not exist |
| `ResourceAlreadyExistsException` | Alias or key id already in use |
| `FailedPreconditionException` | Key is disabled, pending deletion, or the import token expired |
| `InvalidArgumentException` | Invalid ciphertext, malformed policy, or missing required fields |
| `UnAuthorizedException` | Caller lacks permission or credentials are invalid |
| `ResourceExhaustedException` | Throttling or quota limits reached |
| `UnSupportedOperationException` | Feature not available on this provider or configuration |

```java
try {
    kmsClient.decrypt(DecryptRequest.builder()
        .keyRef(keyRef)
        .ciphertext(ciphertext)
        .build());
} catch (FailedPreconditionException e) {
    // key disabled: re-enable or fail the request
} catch (SubstrateSdkException e) {
    // access denied, invalid ciphertext, etc.
}
```
