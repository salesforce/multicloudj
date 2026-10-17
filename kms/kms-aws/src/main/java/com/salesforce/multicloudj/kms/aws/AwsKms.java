package com.salesforce.multicloudj.kms.aws;

import com.google.auto.service.AutoService;
import com.salesforce.multicloudj.common.aws.AwsConstants;
import com.salesforce.multicloudj.common.aws.AwsRetryClassifier;
import com.salesforce.multicloudj.common.aws.AwsRetryStrategies;
import com.salesforce.multicloudj.common.aws.CredentialsProvider;
import com.salesforce.multicloudj.common.exceptions.ExceptionHandler;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.kms.driver.AbstractKms;
import com.salesforce.multicloudj.kms.model.CreateKeyRequest;
import com.salesforce.multicloudj.kms.model.CreateKeyResponse;
import com.salesforce.multicloudj.kms.model.DecryptRequest;
import com.salesforce.multicloudj.kms.model.DecryptResponse;
import com.salesforce.multicloudj.kms.model.EncryptRequest;
import com.salesforce.multicloudj.kms.model.EncryptResponse;
import com.salesforce.multicloudj.kms.model.ExpirationModel;
import com.salesforce.multicloudj.kms.model.GetImportParametersRequest;
import com.salesforce.multicloudj.kms.model.ImportKeyMaterialRequest;
import com.salesforce.multicloudj.kms.model.ImportParameters;
import com.salesforce.multicloudj.kms.model.KeyOrigin;
import com.salesforce.multicloudj.kms.model.KeyPolicy;
import com.salesforce.multicloudj.kms.model.KeyRef;
import com.salesforce.multicloudj.kms.model.KeyState;
import com.salesforce.multicloudj.kms.model.PolicyCapabilities;
import com.salesforce.multicloudj.kms.model.WrappingAlgorithm;
import com.salesforce.multicloudj.kms.model.WrappingKeySpec;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.KmsClientBuilder;
import software.amazon.awssdk.services.kms.model.AlgorithmSpec;
import software.amazon.awssdk.services.kms.model.CreateAliasRequest;
import software.amazon.awssdk.services.kms.model.DisableKeyRequest;
import software.amazon.awssdk.services.kms.model.EnableKeyRequest;
import software.amazon.awssdk.services.kms.model.ExpirationModelType;
import software.amazon.awssdk.services.kms.model.GetKeyPolicyRequest;
import software.amazon.awssdk.services.kms.model.GetParametersForImportRequest;
import software.amazon.awssdk.services.kms.model.GetParametersForImportResponse;
import software.amazon.awssdk.services.kms.model.KeyMetadata;
import software.amazon.awssdk.services.kms.model.KeySpec;
import software.amazon.awssdk.services.kms.model.KeyUsageType;
import software.amazon.awssdk.services.kms.model.OriginType;
import software.amazon.awssdk.services.kms.model.PutKeyPolicyRequest;
import software.amazon.awssdk.services.kms.model.Tag;
import software.amazon.awssdk.services.kms.model.TagResourceRequest;

/** AWS KMS implementation of {@link AbstractKms}. */
@AutoService(AbstractKms.class)
public class AwsKms extends AbstractKms {

  /** KMS key policies are always stored under this fixed name. */
  private static final String POLICY_NAME = "default";

  private static final String ALIAS_PREFIX = "alias/";
  private static final String KEY_ARN_MARKER = ":key/";

  private final KmsClient kmsClient;

  public AwsKms() {
    this(new Builder());
  }

  public AwsKms(Builder builder) {
    super(builder);
    this.kmsClient = builder.kmsClient;
  }

  public static class Builder extends AbstractKms.Builder<AwsKms, Builder> {

    private KmsClient kmsClient;

    public Builder() {
      providerId(AwsConstants.PROVIDER_ID);
    }

    public Builder withKmsClient(KmsClient kmsClient) {
      this.kmsClient = kmsClient;
      return this;
    }

    private static KmsClient buildKmsClient(Builder builder) {
      KmsClientBuilder clientBuilder = KmsClient.builder();
      Region region = null;
      if (builder.getRegion() != null) {
        region = Region.of(builder.getRegion());
        clientBuilder.region(region);
      }
      AwsCredentialsProvider credentialsProvider =
          CredentialsProvider.getCredentialsProvider(builder.getCredentialsOverrider(), region);
      if (credentialsProvider != null) {
        clientBuilder.credentialsProvider(credentialsProvider);
      }
      if (builder.getEndpoint() != null) {
        clientBuilder.endpointOverride(builder.getEndpoint());
      }
      if (builder.getRetryConfig() != null) {
        clientBuilder.overrideConfiguration(
            config -> AwsRetryStrategies.applyRetryConfig(config, builder.getRetryConfig()));
      }
      return clientBuilder.build();
    }

    @Override
    public Builder self() {
      return this;
    }

    @Override
    public AwsKms build() {
      if (this.kmsClient == null) {
        this.kmsClient = buildKmsClient(this);
      }
      return new AwsKms(this);
    }
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
    Class<? extends SubstrateSdkException> exceptionClass;
    if (t instanceof AwsServiceException) {
      AwsServiceException serviceException = (AwsServiceException) t;
      exceptionClass =
          serviceException.awsErrorDetails() != null
              ? ErrorCodeMapping.getException(serviceException.awsErrorDetails().errorCode())
              : UnknownException.class;
    } else if (t instanceof SdkClientException || t instanceof IllegalArgumentException) {
      exceptionClass = InvalidArgumentException.class;
    } else {
      exceptionClass = UnknownException.class;
    }
    return ExceptionHandler.build(exceptionClass, t, AwsRetryClassifier.classify(t));
  }

  @Override
  public PolicyCapabilities getPolicyCapabilities() {
    return PolicyCapabilities.builder().supportsDeny(true).supportsConditions(true).build();
  }

  @Override
  public void close() {
    if (kmsClient != null) {
      kmsClient.close();
    }
  }

  @Override
  protected CreateKeyResponse doCreateKey(CreateKeyRequest request) {
    software.amazon.awssdk.services.kms.model.CreateKeyRequest.Builder nativeRequest =
        software.amazon.awssdk.services.kms.model.CreateKeyRequest.builder()
            .keySpec(KeySpec.SYMMETRIC_DEFAULT)
            .keyUsage(KeyUsageType.ENCRYPT_DECRYPT)
            .origin(
                request.getOrigin() == KeyOrigin.EXTERNAL
                    ? OriginType.EXTERNAL
                    : OriginType.AWS_KMS)
            .multiRegion(request.isMultiRegion())
            .description(request.getDescription());
    if (!request.getTags().isEmpty()) {
      nativeRequest.tags(toTags(request.getTags()));
    }

    KeyMetadata metadata = kmsClient.createKey(nativeRequest.build()).keyMetadata();

    if (StringUtils.isNotBlank(request.getKeyName())) {
      kmsClient.createAlias(
          CreateAliasRequest.builder()
              .aliasName(ALIAS_PREFIX + request.getKeyName())
              .targetKeyId(metadata.keyId())
              .build());
    }

    return CreateKeyResponse.builder()
        .keyRef(
            KeyRef.builder()
                .providerId(getProviderId())
                .region(region)
                .keyId(metadata.keyId())
                .resourceName(metadata.arn())
                .build())
        .keyState(toKeyState(metadata))
        .creationTime(metadata.creationDate())
        .description(metadata.description())
        .build();
  }

  @Override
  protected void doEnableKey(KeyRef keyRef) {
    rejectVersion(keyRef);
    kmsClient.enableKey(EnableKeyRequest.builder().keyId(resolveKeyId(keyRef)).build());
  }

  @Override
  protected void doDisableKey(KeyRef keyRef) {
    rejectVersion(keyRef);
    kmsClient.disableKey(DisableKeyRequest.builder().keyId(resolveKeyId(keyRef)).build());
  }

  @Override
  protected ImportParameters doGetImportParameters(GetImportParametersRequest request) {
    WrappingAlgorithm algorithm = request.getWrappingAlgorithm();
    WrappingKeySpec keySpec =
        request.getWrappingKeySpec() != null
            ? request.getWrappingKeySpec()
            : WrappingKeySpec.RSA_4096;

    GetParametersForImportResponse response =
        kmsClient.getParametersForImport(
            GetParametersForImportRequest.builder()
                .keyId(resolveKeyId(request.getKeyRef()))
                .wrappingAlgorithm(toAlgorithmSpec(algorithm))
                .wrappingKeySpec(toWrappingKeySpec(keySpec))
                .build());

    return ImportParameters.builder()
        .keyRef(request.getKeyRef())
        .wrappingPublicKey(response.publicKey().asByteArray())
        .importToken(Base64.getEncoder().encodeToString(response.importToken().asByteArray()))
        .wrappingAlgorithm(algorithm)
        .wrappingKeySpec(keySpec)
        .tokenExpiry(response.parametersValidTo())
        .build();
  }

  @Override
  protected KeyRef doImportKeyMaterial(ImportKeyMaterialRequest request) {
    software.amazon.awssdk.services.kms.model.ImportKeyMaterialRequest.Builder nativeRequest =
        software.amazon.awssdk.services.kms.model.ImportKeyMaterialRequest.builder()
            .keyId(resolveKeyId(request.getKeyRef()))
            .importToken(
                SdkBytes.fromByteArray(Base64.getDecoder().decode(request.getImportToken())))
            .encryptedKeyMaterial(SdkBytes.fromByteArray(request.getWrappedKeyMaterial()));
    if (request.getExpirationModel() == ExpirationModel.KEY_MATERIAL_EXPIRES) {
      nativeRequest
          .expirationModel(ExpirationModelType.KEY_MATERIAL_EXPIRES)
          .validTo(request.getValidTo());
    } else {
      nativeRequest.expirationModel(ExpirationModelType.KEY_MATERIAL_DOES_NOT_EXPIRE);
    }
    kmsClient.importKeyMaterial(nativeRequest.build());
    return request.getKeyRef();
  }

  @Override
  protected void doSetKeyPolicy(KeyRef keyRef, KeyPolicy policy) {
    kmsClient.putKeyPolicy(
        PutKeyPolicyRequest.builder()
            .keyId(resolveKeyId(keyRef))
            .policyName(POLICY_NAME)
            .policy(AwsKeyPolicyTranslator.toAwsPolicy(policy))
            .build());
  }

  @Override
  protected KeyPolicy doGetKeyPolicy(KeyRef keyRef) {
    String document =
        kmsClient
            .getKeyPolicy(
                GetKeyPolicyRequest.builder()
                    .keyId(resolveKeyId(keyRef))
                    .policyName(POLICY_NAME)
                    .build())
            .policy();
    return AwsKeyPolicyTranslator.fromAwsPolicy(document);
  }

  @Override
  protected void doTagKey(KeyRef keyRef, Map<String, String> tags) {
    kmsClient.tagResource(
        TagResourceRequest.builder().keyId(resolveKeyId(keyRef)).tags(toTags(tags)).build());
  }

  @Override
  protected EncryptResponse doEncrypt(EncryptRequest request) {
    software.amazon.awssdk.services.kms.model.EncryptRequest.Builder nativeRequest =
        software.amazon.awssdk.services.kms.model.EncryptRequest.builder()
            .keyId(resolveKeyId(request.getKeyRef()))
            .plaintext(SdkBytes.fromByteArray(request.getPlaintext()));
    if (!request.getEncryptionContext().isEmpty()) {
      nativeRequest.encryptionContext(request.getEncryptionContext());
    }
    software.amazon.awssdk.services.kms.model.EncryptResponse response =
        kmsClient.encrypt(nativeRequest.build());
    return EncryptResponse.builder()
        .ciphertext(response.ciphertextBlob().asByteArray())
        .keyRef(toKeyRef(response.keyId()))
        .build();
  }

  @Override
  protected DecryptResponse doDecrypt(DecryptRequest request) {
    software.amazon.awssdk.services.kms.model.DecryptRequest.Builder nativeRequest =
        software.amazon.awssdk.services.kms.model.DecryptRequest.builder()
            .ciphertextBlob(SdkBytes.fromByteArray(request.getCiphertext()));
    if (request.getKeyRef() != null) {
      nativeRequest.keyId(resolveKeyId(request.getKeyRef()));
    }
    if (!request.getEncryptionContext().isEmpty()) {
      nativeRequest.encryptionContext(request.getEncryptionContext());
    }
    software.amazon.awssdk.services.kms.model.DecryptResponse response =
        kmsClient.decrypt(nativeRequest.build());
    return DecryptResponse.builder()
        .plaintext(response.plaintext().asByteArray())
        .keyRef(toKeyRef(response.keyId()))
        .build();
  }

  private static String resolveKeyId(KeyRef keyRef) {
    return StringUtils.isNotBlank(keyRef.getResourceName())
        ? keyRef.getResourceName()
        : keyRef.getKeyId();
  }

  /**
   * KMS enables and disables whole keys. Honoring a version-scoped request on the whole key would
   * grant or revoke more than the caller asked for.
   */
  private static void rejectVersion(KeyRef keyRef) {
    if (StringUtils.isNotBlank(keyRef.getVersionId())) {
      throw new UnSupportedOperationException(
          "AWS KMS keys are enabled and disabled as a whole; versionId is not supported");
    }
  }

  private static List<Tag> toTags(Map<String, String> tags) {
    return tags.entrySet().stream()
        .map(e -> Tag.builder().tagKey(e.getKey()).tagValue(e.getValue()).build())
        .toList();
  }

  private KeyRef toKeyRef(String keyArnOrId) {
    if (keyArnOrId == null) {
      return null;
    }
    int marker = keyArnOrId.indexOf(KEY_ARN_MARKER);
    boolean isArn = marker >= 0;
    return KeyRef.builder()
        .providerId(getProviderId())
        .region(region)
        .keyId(isArn ? keyArnOrId.substring(marker + KEY_ARN_MARKER.length()) : keyArnOrId)
        .resourceName(isArn ? keyArnOrId : null)
        .build();
  }

  private static KeyState toKeyState(KeyMetadata metadata) {
    if (metadata.keyState() == null) {
      return KeyState.UNKNOWN;
    }
    switch (metadata.keyState()) {
      case CREATING:
        return KeyState.CREATING;
      case ENABLED:
        return KeyState.ENABLED;
      case DISABLED:
        return KeyState.DISABLED;
      case PENDING_IMPORT:
        return KeyState.PENDING_IMPORT;
      case PENDING_DELETION:
      case PENDING_REPLICA_DELETION:
        return KeyState.PENDING_DELETION;
      case UNAVAILABLE:
        return KeyState.UNAVAILABLE;
      default:
        return KeyState.UNKNOWN;
    }
  }

  private static AlgorithmSpec toAlgorithmSpec(WrappingAlgorithm algorithm) {
    switch (algorithm) {
      case RSAES_OAEP_SHA_256:
        return AlgorithmSpec.RSAES_OAEP_SHA_256;
      case RSA_AES_KEY_WRAP_SHA_256:
        return AlgorithmSpec.RSA_AES_KEY_WRAP_SHA_256;
      default:
        throw new UnSupportedOperationException("Unsupported wrapping algorithm " + algorithm);
    }
  }

  private static software.amazon.awssdk.services.kms.model.WrappingKeySpec toWrappingKeySpec(
      WrappingKeySpec keySpec) {
    switch (keySpec) {
      case RSA_2048:
        return software.amazon.awssdk.services.kms.model.WrappingKeySpec.RSA_2048;
      case RSA_3072:
        return software.amazon.awssdk.services.kms.model.WrappingKeySpec.RSA_3072;
      case RSA_4096:
        return software.amazon.awssdk.services.kms.model.WrappingKeySpec.RSA_4096;
      default:
        throw new UnSupportedOperationException("Unsupported wrapping key spec " + keySpec);
    }
  }
}
