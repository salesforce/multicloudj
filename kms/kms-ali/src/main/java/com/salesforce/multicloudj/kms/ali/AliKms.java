package com.salesforce.multicloudj.kms.ali;

import com.aliyun.kms20160120.Client;
import com.aliyun.kms20160120.models.CreateAliasRequest;
import com.aliyun.kms20160120.models.CreateKeyResponseBody.CreateKeyResponseBodyKeyMetadata;
import com.aliyun.kms20160120.models.DecryptRequest;
import com.aliyun.kms20160120.models.DecryptResponseBody;
import com.aliyun.kms20160120.models.DisableKeyRequest;
import com.aliyun.kms20160120.models.EnableKeyRequest;
import com.aliyun.kms20160120.models.EncryptRequest;
import com.aliyun.kms20160120.models.EncryptResponseBody;
import com.aliyun.kms20160120.models.GetKeyPolicyRequest;
import com.aliyun.kms20160120.models.GetParametersForImportRequest;
import com.aliyun.kms20160120.models.GetParametersForImportResponseBody;
import com.aliyun.kms20160120.models.ImportKeyMaterialRequest;
import com.aliyun.kms20160120.models.SetKeyPolicyRequest;
import com.aliyun.kms20160120.models.TagResourceRequest;
import com.aliyun.tea.TeaException;
import com.aliyun.teaopenapi.models.Config;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.auto.service.AutoService;
import com.salesforce.multicloudj.common.ali.AliConstants;
import com.salesforce.multicloudj.common.ali.AliRetryClassifier;
import com.salesforce.multicloudj.common.exceptions.ExceptionHandler;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.common.retries.RetryConfig;
import com.salesforce.multicloudj.kms.driver.AbstractKms;
import com.salesforce.multicloudj.kms.model.CreateKeyRequest;
import com.salesforce.multicloudj.kms.model.CreateKeyResponse;
import com.salesforce.multicloudj.kms.model.DecryptResponse;
import com.salesforce.multicloudj.kms.model.EncryptResponse;
import com.salesforce.multicloudj.kms.model.ExpirationModel;
import com.salesforce.multicloudj.kms.model.GetImportParametersRequest;
import com.salesforce.multicloudj.kms.model.ImportParameters;
import com.salesforce.multicloudj.kms.model.KeyOrigin;
import com.salesforce.multicloudj.kms.model.KeyPolicy;
import com.salesforce.multicloudj.kms.model.KeyRef;
import com.salesforce.multicloudj.kms.model.KeyState;
import com.salesforce.multicloudj.kms.model.PolicyCapabilities;
import com.salesforce.multicloudj.kms.model.PrincipalType;
import com.salesforce.multicloudj.kms.model.ProtectionLevel;
import com.salesforce.multicloudj.kms.model.WrappingAlgorithm;
import com.salesforce.multicloudj.kms.model.WrappingKeySpec;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.net.URI;
import java.time.Instant;
import java.util.Base64;
import java.util.EnumSet;
import java.util.Map;

/** Alibaba Cloud implementation of key management, backed by the KMS OpenAPI client. */
@AutoService(AbstractKms.class)
public class AliKms extends AbstractKms {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String POLICY_NAME = "default";
  private static final String ALIAS_PREFIX = "alias/";
  private static final String ORIGIN_KMS = "Aliyun_KMS";
  private static final String ORIGIN_EXTERNAL = "EXTERNAL";
  private static final int MAX_TIMEOUT_MILLIS = Integer.MAX_VALUE;

  private final Client kmsClient;

  /** Constructor used by ServiceLoader; the instance is only good for {@link #builder()}. */
  public AliKms() {
    super(new Builder());
    this.kmsClient = null;
  }

  public AliKms(Builder builder) {
    super(builder);
    this.kmsClient = builder.kmsClient != null ? builder.kmsClient : createClient(builder);
  }

  @Override
  public Builder builder() {
    return new Builder();
  }

  @Override
  public SubstrateSdkException mapException(Throwable t) {
    if (t instanceof SubstrateSdkException) {
      return (SubstrateSdkException) t;
    }
    TeaException tea = ErrorCodeMapping.findTeaException(t);
    Boolean retryableHint =
        tea == null ? null : AliRetryClassifier.classifyByStatusCode(tea.getStatusCode());
    return ExceptionHandler.build(ErrorCodeMapping.getException(t), t, retryableHint);
  }

  @Override
  public PolicyCapabilities getPolicyCapabilities() {
    return PolicyCapabilities.builder()
        .supportsDeny(true)
        .supportsConditions(false)
        .supportedPrincipalTypes(
            EnumSet.of(PrincipalType.IAM_ROLE, PrincipalType.SERVICE_ROLE, PrincipalType.ACCOUNT))
        .supportedActions(AliKeyPolicyTranslator.supportedActions())
        .build();
  }

  @Override
  protected CreateKeyResponse doCreateKey(CreateKeyRequest request) {
    if (request.isMultiRegion()) {
      throw new UnSupportedOperationException("multi-region keys are not supported by KMS");
    }
    com.aliyun.kms20160120.models.CreateKeyRequest nativeRequest =
        new com.aliyun.kms20160120.models.CreateKeyRequest()
            .setKeySpec("Aliyun_AES_256")
            .setKeyUsage("ENCRYPT/DECRYPT")
            .setOrigin(request.getOrigin() == KeyOrigin.EXTERNAL ? ORIGIN_EXTERNAL : ORIGIN_KMS)
            .setProtectionLevel(
                request.getProtectionLevel() == ProtectionLevel.HSM ? "HSM" : "SOFTWARE");
    if (request.getDescription() != null) {
      nativeRequest.setDescription(request.getDescription());
    }
    if (!request.getTags().isEmpty()) {
      nativeRequest.setTags(toTagsJson(request.getTags()));
    }
    if (keyContainer != null) {
      nativeRequest.setDKMSInstanceId(keyContainer);
    }

    CreateKeyResponseBodyKeyMetadata metadata =
        invoke(() -> kmsClient.createKey(nativeRequest)).getBody().getKeyMetadata();
    if (request.getKeyName() != null) {
      CreateAliasRequest aliasRequest =
          new CreateAliasRequest()
              .setAliasName(ALIAS_PREFIX + request.getKeyName())
              .setKeyId(metadata.getKeyId());
      invoke(() -> kmsClient.createAlias(aliasRequest));
    }
    return CreateKeyResponse.builder()
        .keyRef(keyRef(metadata.getKeyId(), metadata.getArn(), null))
        .keyState(toKeyState(metadata.getKeyState()))
        .creationTime(parseInstant(metadata.getCreationDate()))
        .description(metadata.getDescription())
        .build();
  }

  @Override
  protected void doEnableKey(KeyRef keyRef) {
    rejectVersion(keyRef, "enableKey");
    EnableKeyRequest request = new EnableKeyRequest().setKeyId(keyRef.getKeyId());
    invoke(() -> kmsClient.enableKey(request));
  }

  @Override
  protected void doDisableKey(KeyRef keyRef) {
    rejectVersion(keyRef, "disableKey");
    DisableKeyRequest request = new DisableKeyRequest().setKeyId(keyRef.getKeyId());
    invoke(() -> kmsClient.disableKey(request));
  }

  @Override
  protected ImportParameters doGetImportParameters(GetImportParametersRequest request) {
    if (request.getWrappingAlgorithm() != WrappingAlgorithm.RSAES_OAEP_SHA_256) {
      throw new UnSupportedOperationException(
          "wrapping algorithm " + request.getWrappingAlgorithm() + " is not supported by KMS");
    }
    WrappingKeySpec spec =
        request.getWrappingKeySpec() != null
            ? request.getWrappingKeySpec()
            : WrappingKeySpec.RSA_2048;
    GetParametersForImportRequest nativeRequest =
        new GetParametersForImportRequest()
            .setKeyId(request.getKeyRef().getKeyId())
            .setWrappingAlgorithm("RSAES_OAEP_SHA_256")
            .setWrappingKeySpec(spec.name());

    GetParametersForImportResponseBody body =
        invoke(() -> kmsClient.getParametersForImport(nativeRequest)).getBody();
    return ImportParameters.builder()
        .keyRef(request.getKeyRef())
        .wrappingPublicKey(Base64.getDecoder().decode(body.getPublicKey()))
        .importToken(body.getImportToken())
        .wrappingAlgorithm(WrappingAlgorithm.RSAES_OAEP_SHA_256)
        .wrappingKeySpec(spec)
        .tokenExpiry(parseInstant(body.getTokenExpireTime()))
        .build();
  }

  @Override
  protected KeyRef doImportKeyMaterial(
      com.salesforce.multicloudj.kms.model.ImportKeyMaterialRequest request) {
    long expireUnix =
        request.getExpirationModel() == ExpirationModel.KEY_MATERIAL_EXPIRES
            ? request.getValidTo().getEpochSecond()
            : 0L;
    ImportKeyMaterialRequest nativeRequest =
        new ImportKeyMaterialRequest()
            .setKeyId(request.getKeyRef().getKeyId())
            .setImportToken(request.getImportToken())
            .setEncryptedKeyMaterial(
                Base64.getEncoder().encodeToString(request.getWrappedKeyMaterial()))
            .setKeyMaterialExpireUnix(expireUnix);
    invoke(() -> kmsClient.importKeyMaterial(nativeRequest));
    return request.getKeyRef();
  }

  @Override
  protected void doSetKeyPolicy(KeyRef keyRef, KeyPolicy policy) {
    SetKeyPolicyRequest request =
        new SetKeyPolicyRequest()
            .setKeyId(keyRef.getKeyId())
            .setPolicyName(POLICY_NAME)
            .setPolicy(AliKeyPolicyTranslator.toPolicyJson(policy));
    invoke(() -> kmsClient.setKeyPolicy(request));
  }

  @Override
  protected KeyPolicy doGetKeyPolicy(KeyRef keyRef) {
    GetKeyPolicyRequest request =
        new GetKeyPolicyRequest().setKeyId(keyRef.getKeyId()).setPolicyName(POLICY_NAME);
    String policy = invoke(() -> kmsClient.getKeyPolicy(request)).getBody().getPolicy();
    return AliKeyPolicyTranslator.fromPolicyJson(policy);
  }

  @Override
  protected void doTagKey(KeyRef keyRef, Map<String, String> tags) {
    TagResourceRequest request =
        new TagResourceRequest().setKeyId(keyRef.getKeyId()).setTags(toTagsJson(tags));
    invoke(() -> kmsClient.tagResource(request));
  }

  /**
   * The Encrypt API carries the plaintext as a string, so arbitrary bytes are sent base64-encoded
   * and Decrypt's result is base64-decoded. Ciphertexts therefore round-trip only through this
   * implementation.
   */
  @Override
  protected EncryptResponse doEncrypt(com.salesforce.multicloudj.kms.model.EncryptRequest request) {
    EncryptRequest nativeRequest =
        new EncryptRequest()
            .setKeyId(request.getKeyRef().getKeyId())
            .setPlaintext(Base64.getEncoder().encodeToString(request.getPlaintext()));
    if (!request.getEncryptionContext().isEmpty()) {
      nativeRequest.setEncryptionContext(request.getEncryptionContext());
    }
    EncryptResponseBody body = invoke(() -> kmsClient.encrypt(nativeRequest)).getBody();
    return EncryptResponse.builder()
        .ciphertext(Base64.getDecoder().decode(body.getCiphertextBlob()))
        .keyRef(keyRef(body.getKeyId(), null, body.getKeyVersionId()))
        .build();
  }

  @Override
  protected DecryptResponse doDecrypt(com.salesforce.multicloudj.kms.model.DecryptRequest request) {
    DecryptRequest nativeRequest =
        new DecryptRequest()
            .setCiphertextBlob(Base64.getEncoder().encodeToString(request.getCiphertext()));
    if (!request.getEncryptionContext().isEmpty()) {
      nativeRequest.setEncryptionContext(request.getEncryptionContext());
    }
    DecryptResponseBody body = invoke(() -> kmsClient.decrypt(nativeRequest)).getBody();
    return DecryptResponse.builder()
        .plaintext(Base64.getDecoder().decode(body.getPlaintext()))
        .keyRef(keyRef(body.getKeyId(), null, body.getKeyVersionId()))
        .build();
  }

  /** The KMS client holds no closeable resources; HTTP connections are pooled process-wide. */
  @Override
  public void close() {}

  private KeyRef keyRef(String keyId, String arn, String versionId) {
    return KeyRef.builder()
        .providerId(AliConstants.PROVIDER_ID)
        .region(region)
        .keyId(keyId)
        .resourceName(arn)
        .versionId(versionId)
        .build();
  }

  private static void rejectVersion(KeyRef keyRef, String operation) {
    if (keyRef.getVersionId() != null) {
      throw new UnSupportedOperationException(
          operation + " applies to a whole key; key versions cannot be addressed");
    }
  }

  private static KeyState toKeyState(String nativeState) {
    if (nativeState == null) {
      return KeyState.UNKNOWN;
    }
    return switch (nativeState) {
      case "Enabled" -> KeyState.ENABLED;
      case "Disabled" -> KeyState.DISABLED;
      case "PendingDeletion" -> KeyState.PENDING_DELETION;
      case "PendingImport" -> KeyState.PENDING_IMPORT;
      default -> KeyState.UNKNOWN;
    };
  }

  private static Instant parseInstant(String value) {
    return value == null || value.isBlank() ? null : Instant.parse(value);
  }

  private static String toTagsJson(Map<String, String> tags) {
    ArrayNode array = MAPPER.createArrayNode();
    tags.forEach(
        (key, value) -> {
          ObjectNode tag = array.addObject();
          tag.put("TagKey", key);
          tag.put("TagValue", value);
        });
    try {
      return MAPPER.writeValueAsString(array);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("failed to serialize tags", e);
    }
  }

  /** A call into the KMS client, whose methods declare the checked {@link Exception}. */
  @FunctionalInterface
  private interface KmsCall<T> {
    T call() throws Exception;
  }

  /**
   * Runs a client call. Unchecked failures, including {@link TeaException}, propagate unchanged;
   * checked ones are wrapped with the original as the cause so that classification stays in {@link
   * #mapException}.
   */
  private static <T> T invoke(KmsCall<T> call) {
    try {
      return call.call();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new KmsCallException(e);
    }
  }

  /** Carries a checked exception thrown by the KMS client. */
  static final class KmsCallException extends RuntimeException {
    KmsCallException(Throwable cause) {
      super(cause);
    }
  }

  private static Client createClient(Builder builder) {
    if (builder.getRegion() == null && builder.getEndpoint() == null) {
      throw new InvalidArgumentException("region or endpoint is required");
    }
    Config config = new Config();
    URI endpoint = builder.getEndpoint();
    if (endpoint != null) {
      config.setEndpoint(endpoint.getAuthority());
      if (endpoint.getScheme() != null) {
        config.setProtocol(endpoint.getScheme());
      }
    } else {
      config.setEndpoint("kms." + builder.getRegion() + ".aliyuncs.com");
    }
    if (builder.getRegion() != null) {
      config.setRegionId(builder.getRegion());
    }
    RetryConfig retryConfig = builder.getRetryConfig();
    if (retryConfig != null && retryConfig.getAttemptTimeout() != null) {
      int timeout = (int) Math.min(retryConfig.getAttemptTimeout(), MAX_TIMEOUT_MILLIS);
      config.setConnectTimeout(timeout);
      config.setReadTimeout(timeout);
    }
    applyCredentials(config, builder.getCredentialsOverrider());
    try {
      return new Client(config);
    } catch (Exception e) {
      throw new KmsCallException(e);
    }
  }

  /** Without an override the SDK's default credential chain resolves credentials per request. */
  private static void applyCredentials(Config config, CredentialsOverrider overrider) {
    if (overrider == null || overrider.getType() == null) {
      try {
        config.setCredential(new com.aliyun.credentials.Client());
      } catch (Exception e) {
        throw new KmsCallException(e);
      }
      return;
    }
    if (overrider.getType() != CredentialsType.SESSION) {
      throw new UnSupportedOperationException(
          "credentials type " + overrider.getType() + " is not supported");
    }
    StsCredentials credentials = overrider.getSessionCredentials();
    config.setAccessKeyId(credentials.getAccessKeyId());
    config.setAccessKeySecret(credentials.getAccessKeySecret());
    if (credentials.getSecurityToken() != null) {
      config.setSecurityToken(credentials.getSecurityToken());
      config.setType("sts");
    }
  }

  public static class Builder extends AbstractKms.Builder<AliKms, Builder> {
    private Client kmsClient;

    public Builder() {
      providerId(AliConstants.PROVIDER_ID);
    }

    /**
     * Supplies a preconfigured KMS client, which takes precedence over region, endpoint,
     * credentials and retry settings.
     *
     * @param kmsClient the client to use
     * @return An instance of self
     */
    public Builder withKmsClient(Client kmsClient) {
      this.kmsClient = kmsClient;
      return this;
    }

    @Override
    public Builder self() {
      return this;
    }

    @Override
    public AliKms build() {
      return new AliKms(this);
    }
  }
}
