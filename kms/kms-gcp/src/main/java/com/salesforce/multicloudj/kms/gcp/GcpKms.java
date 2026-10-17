package com.salesforce.multicloudj.kms.gcp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.ApiException;
import com.google.auth.Credentials;
import com.google.auto.service.AutoService;
import com.google.cloud.kms.v1.CreateCryptoKeyRequest;
import com.google.cloud.kms.v1.CreateImportJobRequest;
import com.google.cloud.kms.v1.CryptoKey;
import com.google.cloud.kms.v1.CryptoKey.CryptoKeyPurpose;
import com.google.cloud.kms.v1.CryptoKeyVersion;
import com.google.cloud.kms.v1.CryptoKeyVersion.CryptoKeyVersionAlgorithm;
import com.google.cloud.kms.v1.CryptoKeyVersion.CryptoKeyVersionState;
import com.google.cloud.kms.v1.CryptoKeyVersionTemplate;
import com.google.cloud.kms.v1.ImportCryptoKeyVersionRequest;
import com.google.cloud.kms.v1.ImportJob;
import com.google.cloud.kms.v1.ImportJob.ImportJobState;
import com.google.cloud.kms.v1.ImportJob.ImportMethod;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.cloud.kms.v1.KeyManagementServiceSettings;
import com.google.cloud.kms.v1.UpdateCryptoKeyPrimaryVersionRequest;
import com.google.cloud.kms.v1.UpdateCryptoKeyRequest;
import com.google.cloud.kms.v1.UpdateCryptoKeyVersionRequest;
import com.google.iam.v1.GetIamPolicyRequest;
import com.google.iam.v1.Policy;
import com.google.iam.v1.SetIamPolicyRequest;
import com.google.protobuf.ByteString;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Timestamp;
import com.salesforce.multicloudj.common.exceptions.DeadlineExceededException;
import com.salesforce.multicloudj.common.exceptions.ExceptionHandler;
import com.salesforce.multicloudj.common.exceptions.FailedPreconditionException;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.common.gcp.GcpConstants;
import com.salesforce.multicloudj.common.gcp.GcpCredentialsProvider;
import com.salesforce.multicloudj.common.gcp.GcpRetryClassifier;
import com.salesforce.multicloudj.common.retries.RetryConfig;
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
import com.salesforce.multicloudj.kms.model.ProtectionLevel;
import com.salesforce.multicloudj.kms.model.WrappingAlgorithm;
import com.salesforce.multicloudj.kms.model.WrappingKeySpec;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.lang3.StringUtils;

/**
 * Cloud KMS implementation of {@link AbstractKms}.
 *
 * <p>Keys are Cloud KMS crypto keys. The key container is the key ring resource name ({@code
 * projects/<project>/locations/<location>/keyRings/<ring>}); a key id that does not start with
 * {@code projects/} is resolved against it.
 */
@AutoService(AbstractKms.class)
public class GcpKms extends AbstractKms {

  private static final String PROJECTS_PREFIX = "projects/";
  private static final String CRYPTO_KEYS_SEGMENT = "/cryptoKeys/";
  private static final String VERSIONS_SEGMENT = "/cryptoKeyVersions/";
  private static final String IMPORT_JOBS_SEGMENT = "/importJobs/";
  private static final String IMPORT_JOB_ID_PREFIX = "mcj-";
  // Import job ids are limited to 63 characters.
  private static final int MAX_IMPORT_JOB_ID_LENGTH = 63;
  private static final long POLL_INTERVAL_MILLIS = 1000;
  private static final int MAX_POLLS = 120;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /** Pauses between polls of asynchronous Cloud KMS operations. */
  @FunctionalInterface
  interface Sleeper {
    void sleep(long millis) throws InterruptedException;
  }

  private final KeyManagementServiceClient kmsClient;
  private final Sleeper sleeper;

  public GcpKms() {
    this(new Builder());
  }

  public GcpKms(Builder builder) {
    super(builder);
    this.kmsClient = builder.kmsClient;
    this.sleeper = builder.sleeper;
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
    Class<? extends SubstrateSdkException> exceptionClass = UnknownException.class;
    if (t instanceof ApiException && ((ApiException) t).getStatusCode() != null) {
      exceptionClass = ErrorCodeMapping.getException(((ApiException) t).getStatusCode().getCode());
    }
    return ExceptionHandler.build(exceptionClass, t, GcpRetryClassifier.classify(t));
  }

  @Override
  public PolicyCapabilities getPolicyCapabilities() {
    return GcpKeyPolicyTranslator.CAPABILITIES;
  }

  @Override
  public void close() {
    if (kmsClient != null) {
      kmsClient.close();
    }
  }

  @Override
  protected CreateKeyResponse doCreateKey(CreateKeyRequest request) {
    if (StringUtils.isBlank(request.getKeyName())) {
      throw new InvalidArgumentException("keyName is required: it becomes the crypto key id");
    }
    if (request.isMultiRegion()) {
      throw new UnSupportedOperationException(
          "multiRegion keys are not supported: a key's location is fixed by its key ring");
    }
    if (StringUtils.isBlank(keyContainer)) {
      throw new InvalidArgumentException("a key container (key ring) is required to create keys");
    }

    boolean external = request.getOrigin() == KeyOrigin.EXTERNAL;
    CryptoKey.Builder cryptoKey =
        CryptoKey.newBuilder()
            .setPurpose(CryptoKeyPurpose.ENCRYPT_DECRYPT)
            .setVersionTemplate(
                CryptoKeyVersionTemplate.newBuilder()
                    .setAlgorithm(CryptoKeyVersionAlgorithm.GOOGLE_SYMMETRIC_ENCRYPTION)
                    .setProtectionLevel(toProtectionLevel(request.getProtectionLevel())))
            .putAllLabels(request.getTags());
    if (external) {
      cryptoKey.setImportOnly(true);
    }

    CryptoKey created =
        kmsClient.createCryptoKey(
            CreateCryptoKeyRequest.newBuilder()
                .setParent(keyContainer)
                .setCryptoKeyId(request.getKeyName())
                .setCryptoKey(cryptoKey)
                .setSkipInitialVersionCreation(external)
                .build());

    KeyState state = external ? KeyState.PENDING_IMPORT : toKeyState(created);
    return CreateKeyResponse.builder()
        .keyRef(keyRefFor(created.getName(), null))
        .keyState(state)
        .creationTime(created.hasCreateTime() ? toInstant(created.getCreateTime()) : null)
        .build();
  }

  @Override
  protected void doEnableKey(KeyRef keyRef) {
    changeVersionStates(keyRef, CryptoKeyVersionState.DISABLED, CryptoKeyVersionState.ENABLED);
  }

  @Override
  protected void doDisableKey(KeyRef keyRef) {
    changeVersionStates(keyRef, CryptoKeyVersionState.ENABLED, CryptoKeyVersionState.DISABLED);
  }

  @Override
  protected ImportParameters doGetImportParameters(GetImportParametersRequest request) {
    String keyName = resolveKeyName(request.getKeyRef());
    WrappingAlgorithm algorithm = request.getWrappingAlgorithm();
    WrappingKeySpec keySpec =
        request.getWrappingKeySpec() == null
            ? WrappingKeySpec.RSA_3072
            : request.getWrappingKeySpec();
    ImportMethod method = toImportMethod(algorithm, keySpec);

    CryptoKey cryptoKey = kmsClient.getCryptoKey(keyName);
    com.google.cloud.kms.v1.ProtectionLevel protectionLevel =
        cryptoKey.getVersionTemplate().getProtectionLevel();
    if (protectionLevel == com.google.cloud.kms.v1.ProtectionLevel.PROTECTION_LEVEL_UNSPECIFIED) {
      protectionLevel = com.google.cloud.kms.v1.ProtectionLevel.SOFTWARE;
    }

    String keyRing = keyName.substring(0, keyName.indexOf(CRYPTO_KEYS_SEGMENT));
    String importJobId = importJobId(lastSegment(keyName), method);
    ImportJob job = createOrReuseImportJob(keyRing, importJobId, method, protectionLevel);
    ImportJob active = awaitImportJobActive(job);

    return ImportParameters.builder()
        .keyRef(request.getKeyRef())
        .wrappingPublicKey(pemToDer(active.getPublicKey().getPem()))
        .importToken(active.getName())
        .wrappingAlgorithm(algorithm)
        .wrappingKeySpec(keySpec)
        .tokenExpiry(active.hasExpireTime() ? toInstant(active.getExpireTime()) : null)
        .build();
  }

  @Override
  protected KeyRef doImportKeyMaterial(ImportKeyMaterialRequest request) {
    if (request.getExpirationModel() == ExpirationModel.KEY_MATERIAL_EXPIRES) {
      throw new UnSupportedOperationException(
          "imported key material cannot be given an expiry in Cloud KMS");
    }
    String keyName = resolveKeyName(request.getKeyRef());

    // The import job's method decides which request field carries the wrapped material.
    ImportJob job = kmsClient.getImportJob(request.getImportToken());
    ImportCryptoKeyVersionRequest.Builder importRequest =
        ImportCryptoKeyVersionRequest.newBuilder()
            .setParent(keyName)
            .setAlgorithm(CryptoKeyVersionAlgorithm.GOOGLE_SYMMETRIC_ENCRYPTION)
            .setImportJob(request.getImportToken());
    ByteString wrapped = ByteString.copyFrom(request.getWrappedKeyMaterial());
    if (usesAesKeyWrap(job.getImportMethod())) {
      importRequest.setRsaAesWrappedKey(wrapped);
    } else {
      importRequest.setWrappedKey(wrapped);
    }

    CryptoKeyVersion version =
        awaitVersionImported(kmsClient.importCryptoKeyVersion(importRequest.build()));

    CryptoKey cryptoKey = kmsClient.getCryptoKey(keyName);
    if (!cryptoKey.hasPrimary()) {
      kmsClient.updateCryptoKeyPrimaryVersion(
          UpdateCryptoKeyPrimaryVersionRequest.newBuilder()
              .setName(keyName)
              .setCryptoKeyVersionId(lastSegment(version.getName()))
              .build());
    }
    return keyRefFor(keyName, lastSegment(version.getName()));
  }

  @Override
  protected void doSetKeyPolicy(KeyRef keyRef, KeyPolicy policy) {
    String keyName = resolveKeyName(keyRef);
    Policy current =
        kmsClient.getIamPolicy(GetIamPolicyRequest.newBuilder().setResource(keyName).build());
    Policy replacement =
        Policy.newBuilder()
            .setEtag(current.getEtag())
            .addAllBindings(GcpKeyPolicyTranslator.toBindings(policy))
            .build();
    kmsClient.setIamPolicy(
        SetIamPolicyRequest.newBuilder().setResource(keyName).setPolicy(replacement).build());
  }

  @Override
  protected KeyPolicy doGetKeyPolicy(KeyRef keyRef) {
    Policy policy =
        kmsClient.getIamPolicy(
            GetIamPolicyRequest.newBuilder().setResource(resolveKeyName(keyRef)).build());
    return GcpKeyPolicyTranslator.toKeyPolicy(policy);
  }

  @Override
  protected void doTagKey(KeyRef keyRef, Map<String, String> tags) {
    String keyName = resolveKeyName(keyRef);
    Map<String, String> labels = new HashMap<>(kmsClient.getCryptoKey(keyName).getLabelsMap());
    labels.putAll(tags);
    kmsClient.updateCryptoKey(
        UpdateCryptoKeyRequest.newBuilder()
            .setCryptoKey(CryptoKey.newBuilder().setName(keyName).putAllLabels(labels))
            .setUpdateMask(FieldMask.newBuilder().addPaths("labels"))
            .build());
  }

  @Override
  protected EncryptResponse doEncrypt(EncryptRequest request) {
    KeyRef keyRef = request.getKeyRef();
    String keyName = resolveKeyName(keyRef);
    String versionId = resolveVersionId(keyRef);
    String target = versionId == null ? keyName : keyName + VERSIONS_SEGMENT + versionId;

    com.google.cloud.kms.v1.EncryptRequest.Builder encrypt =
        com.google.cloud.kms.v1.EncryptRequest.newBuilder()
            .setName(target)
            .setPlaintext(ByteString.copyFrom(request.getPlaintext()));
    ByteString aad = encodeEncryptionContext(request.getEncryptionContext());
    if (!aad.isEmpty()) {
      encrypt.setAdditionalAuthenticatedData(aad);
    }

    com.google.cloud.kms.v1.EncryptResponse response = kmsClient.encrypt(encrypt.build());

    String usedName = response.getName().isEmpty() ? target : response.getName();
    int versionStart = usedName.indexOf(VERSIONS_SEGMENT);
    KeyRef used =
        versionStart < 0
            ? keyRefFor(usedName, null)
            : keyRefFor(usedName.substring(0, versionStart), lastSegment(usedName));
    return EncryptResponse.builder()
        .ciphertext(response.getCiphertext().toByteArray())
        .keyRef(used)
        .build();
  }

  @Override
  protected DecryptResponse doDecrypt(DecryptRequest request) {
    if (request.getKeyRef() == null) {
      throw new InvalidArgumentException("keyRef is required to decrypt");
    }
    String keyName = resolveKeyName(request.getKeyRef());

    com.google.cloud.kms.v1.DecryptRequest.Builder decrypt =
        com.google.cloud.kms.v1.DecryptRequest.newBuilder()
            .setName(keyName)
            .setCiphertext(ByteString.copyFrom(request.getCiphertext()));
    ByteString aad = encodeEncryptionContext(request.getEncryptionContext());
    if (!aad.isEmpty()) {
      decrypt.setAdditionalAuthenticatedData(aad);
    }

    com.google.cloud.kms.v1.DecryptResponse response = kmsClient.decrypt(decrypt.build());
    return DecryptResponse.builder()
        .plaintext(response.getPlaintext().toByteArray())
        .keyRef(keyRefFor(keyName, null))
        .build();
  }

  /**
   * Moves versions from {@code from} to {@code to}: only the addressed version when the reference
   * names one, otherwise every version currently in {@code from}. Destroyed and pending versions
   * are left alone.
   */
  private void changeVersionStates(
      KeyRef keyRef, CryptoKeyVersionState from, CryptoKeyVersionState to) {
    String keyName = resolveKeyName(keyRef);
    String versionId = resolveVersionId(keyRef);
    if (versionId != null) {
      updateVersionState(keyName + VERSIONS_SEGMENT + versionId, to);
      return;
    }
    List<CryptoKeyVersion> versions = new java.util.ArrayList<>();
    kmsClient.listCryptoKeyVersions(keyName).iterateAll().forEach(versions::add);
    for (CryptoKeyVersion version : versions) {
      if (version.getState() == from) {
        updateVersionState(version.getName(), to);
      }
    }
  }

  private void updateVersionState(String versionName, CryptoKeyVersionState state) {
    kmsClient.updateCryptoKeyVersion(
        UpdateCryptoKeyVersionRequest.newBuilder()
            .setCryptoKeyVersion(CryptoKeyVersion.newBuilder().setName(versionName).setState(state))
            .setUpdateMask(FieldMask.newBuilder().addPaths("state"))
            .build());
  }

  /**
   * Creates the import job with its deterministic id. An existing job with that id is reused, so
   * repeated requests for the same key and method share one wrapping key.
   */
  private ImportJob createOrReuseImportJob(
      String keyRing,
      String importJobId,
      ImportMethod method,
      com.google.cloud.kms.v1.ProtectionLevel protectionLevel) {
    try {
      return kmsClient.createImportJob(
          CreateImportJobRequest.newBuilder()
              .setParent(keyRing)
              .setImportJobId(importJobId)
              .setImportJob(
                  ImportJob.newBuilder()
                      .setImportMethod(method)
                      .setProtectionLevel(protectionLevel))
              .build());
    } catch (AlreadyExistsException e) {
      return kmsClient.getImportJob(keyRing + IMPORT_JOBS_SEGMENT + importJobId);
    }
  }

  private ImportJob awaitImportJobActive(ImportJob job) {
    ImportJob current = job;
    for (int poll = 0; ; poll++) {
      if (current.getState() == ImportJobState.ACTIVE) {
        return current;
      }
      if (current.getState() == ImportJobState.EXPIRED) {
        throw new FailedPreconditionException(
            "import job " + current.getName() + " has expired and its id cannot be reused");
      }
      if (poll >= MAX_POLLS) {
        throw new DeadlineExceededException(
            "import job " + current.getName() + " did not become active");
      }
      pause();
      current = kmsClient.getImportJob(job.getName());
    }
  }

  private CryptoKeyVersion awaitVersionImported(CryptoKeyVersion imported) {
    CryptoKeyVersion current = imported;
    for (int poll = 0; ; poll++) {
      CryptoKeyVersionState state = current.getState();
      if (state == CryptoKeyVersionState.IMPORT_FAILED) {
        throw new FailedPreconditionException(
            "key material import failed: " + current.getImportFailureReason());
      }
      if (state != CryptoKeyVersionState.PENDING_IMPORT
          && state != CryptoKeyVersionState.PENDING_GENERATION) {
        return current;
      }
      if (poll >= MAX_POLLS) {
        throw new DeadlineExceededException(
            "key version " + current.getName() + " did not finish importing");
      }
      pause();
      current = kmsClient.getCryptoKeyVersion(imported.getName());
    }
  }

  private void pause() {
    try {
      sleeper.sleep(POLL_INTERVAL_MILLIS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SubstrateSdkException("interrupted while waiting for Cloud KMS", e);
    }
  }

  /** Resolves a reference to the crypto key resource name, dropping any version. */
  String resolveKeyName(KeyRef keyRef) {
    String candidate =
        StringUtils.isNotBlank(keyRef.getResourceName())
            ? keyRef.getResourceName()
            : keyRef.getKeyId();
    String name;
    if (candidate.startsWith(PROJECTS_PREFIX)) {
      name = candidate;
    } else if (StringUtils.isBlank(keyContainer)) {
      throw new InvalidArgumentException(
          "key id '" + candidate + "' is not a full resource name and no key container is set");
    } else {
      name = StringUtils.removeEnd(keyContainer, "/") + CRYPTO_KEYS_SEGMENT + candidate;
    }
    int versionStart = name.indexOf(VERSIONS_SEGMENT);
    return versionStart < 0 ? name : name.substring(0, versionStart);
  }

  /** Version addressed by the reference, or null when it addresses the key as a whole. */
  private static String resolveVersionId(KeyRef keyRef) {
    if (StringUtils.isNotBlank(keyRef.getVersionId())) {
      return lastSegment(keyRef.getVersionId());
    }
    for (String candidate : new String[] {keyRef.getResourceName(), keyRef.getKeyId()}) {
      if (candidate != null && candidate.contains(VERSIONS_SEGMENT)) {
        return lastSegment(candidate);
      }
    }
    return null;
  }

  private KeyRef keyRefFor(String keyName, String versionId) {
    return KeyRef.builder()
        .providerId(GcpConstants.PROVIDER_ID)
        .region(regionOf(keyName))
        .keyId(keyName)
        .resourceName(keyName)
        .versionId(versionId)
        .build();
  }

  private String regionOf(String keyName) {
    String[] segments = keyName.split("/");
    for (int i = 0; i < segments.length - 1; i++) {
      if ("locations".equals(segments[i])) {
        return segments[i + 1];
      }
    }
    return region;
  }

  private static String lastSegment(String name) {
    return name.substring(name.lastIndexOf('/') + 1);
  }

  private static Instant toInstant(Timestamp timestamp) {
    return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
  }

  private static com.google.cloud.kms.v1.ProtectionLevel toProtectionLevel(ProtectionLevel level) {
    return level == ProtectionLevel.HSM
        ? com.google.cloud.kms.v1.ProtectionLevel.HSM
        : com.google.cloud.kms.v1.ProtectionLevel.SOFTWARE;
  }

  private static KeyState toKeyState(CryptoKey cryptoKey) {
    if (!cryptoKey.hasPrimary()) {
      return KeyState.UNKNOWN;
    }
    switch (cryptoKey.getPrimary().getState()) {
      case ENABLED:
        return KeyState.ENABLED;
      case PENDING_GENERATION:
        return KeyState.CREATING;
      case DISABLED:
        return KeyState.DISABLED;
      case DESTROY_SCHEDULED:
        return KeyState.PENDING_DELETION;
      case PENDING_IMPORT:
        return KeyState.PENDING_IMPORT;
      default:
        return KeyState.UNKNOWN;
    }
  }

  static ImportMethod toImportMethod(WrappingAlgorithm algorithm, WrappingKeySpec keySpec) {
    boolean aesWrap = algorithm == WrappingAlgorithm.RSA_AES_KEY_WRAP_SHA_256;
    switch (keySpec) {
      case RSA_3072:
        return aesWrap
            ? ImportMethod.RSA_OAEP_3072_SHA256_AES_256
            : ImportMethod.RSA_OAEP_3072_SHA256;
      case RSA_4096:
        return aesWrap
            ? ImportMethod.RSA_OAEP_4096_SHA256_AES_256
            : ImportMethod.RSA_OAEP_4096_SHA256;
      default:
        throw new UnSupportedOperationException(
            "wrapping key size " + keySpec + " is not supported: use RSA_3072 or RSA_4096");
    }
  }

  private static boolean usesAesKeyWrap(ImportMethod method) {
    return method == ImportMethod.RSA_OAEP_3072_SHA256_AES_256
        || method == ImportMethod.RSA_OAEP_4096_SHA256_AES_256;
  }

  /**
   * Import job ids are derived from the key id and import method so that the same request always
   * addresses the same job.
   */
  static String importJobId(String cryptoKeyId, ImportMethod method) {
    String suffix = "-" + method.name().toLowerCase(java.util.Locale.ROOT).replace("_", "");
    String base = cryptoKeyId;
    int maxBase = MAX_IMPORT_JOB_ID_LENGTH - IMPORT_JOB_ID_PREFIX.length() - suffix.length();
    if (base.length() > maxBase) {
      String hash = Integer.toHexString(cryptoKeyId.hashCode());
      base = base.substring(0, maxBase - hash.length() - 1) + "-" + hash;
    }
    return IMPORT_JOB_ID_PREFIX + base + suffix;
  }

  /** Converts a PEM-encoded public key to the DER bytes of its SubjectPublicKeyInfo. */
  static byte[] pemToDer(String pem) {
    String body = pem.replaceAll("-----(BEGIN|END) [A-Z ]+-----", "").replaceAll("\\s", "");
    return Base64.getDecoder().decode(body);
  }

  /**
   * Encodes the encryption context as JSON with keys in sorted order so that the same entries
   * always produce the same additional authenticated data, whatever the map's iteration order.
   */
  static ByteString encodeEncryptionContext(Map<String, String> encryptionContext) {
    if (encryptionContext == null || encryptionContext.isEmpty()) {
      return ByteString.EMPTY;
    }
    try {
      return ByteString.copyFromUtf8(
          OBJECT_MAPPER.writeValueAsString(new TreeMap<>(encryptionContext)));
    } catch (JsonProcessingException e) {
      throw new InvalidArgumentException("encryption context cannot be encoded", e);
    }
  }

  static RetrySettings toRetrySettings(RetrySettings base, RetryConfig retryConfig) {
    RetrySettings.Builder settings = base.toBuilder();
    if (retryConfig.getMaxAttempts() != null) {
      settings.setMaxAttempts(retryConfig.getMaxAttempts());
    }
    if (retryConfig.getMode() == RetryConfig.Mode.EXPONENTIAL) {
      settings
          .setInitialRetryDelayDuration(Duration.ofMillis(retryConfig.getInitialDelayMillis()))
          .setRetryDelayMultiplier(retryConfig.getMultiplier())
          .setMaxRetryDelayDuration(Duration.ofMillis(retryConfig.getMaxDelayMillis()));
    } else if (retryConfig.getMode() == RetryConfig.Mode.FIXED) {
      settings
          .setInitialRetryDelayDuration(Duration.ofMillis(retryConfig.getFixedDelayMillis()))
          .setRetryDelayMultiplier(1.0)
          .setMaxRetryDelayDuration(Duration.ofMillis(retryConfig.getFixedDelayMillis()));
    }
    if (retryConfig.getTotalTimeout() != null) {
      settings.setTotalTimeoutDuration(Duration.ofMillis(retryConfig.getTotalTimeout()));
    }
    if (retryConfig.getAttemptTimeout() != null) {
      Duration attemptTimeout = Duration.ofMillis(retryConfig.getAttemptTimeout());
      settings
          .setInitialRpcTimeoutDuration(attemptTimeout)
          .setRpcTimeoutMultiplier(1.0)
          .setMaxRpcTimeoutDuration(attemptTimeout);
    }
    return settings.build();
  }

  public static class Builder extends AbstractKms.Builder<GcpKms, Builder> {
    private KeyManagementServiceClient kmsClient;
    private Sleeper sleeper = Thread::sleep;

    public Builder() {
      providerId(GcpConstants.PROVIDER_ID);
    }

    /**
     * Supplies the Cloud KMS client to use instead of building one from the endpoint, credentials
     * and retry configuration. The built {@link GcpKms} closes it on {@code close()}.
     *
     * @param kmsClient the client to use
     * @return this builder
     */
    public Builder withKmsClient(KeyManagementServiceClient kmsClient) {
      this.kmsClient = kmsClient;
      return this;
    }

    Builder withSleeper(Sleeper sleeper) {
      this.sleeper = sleeper;
      return this;
    }

    @Override
    public Builder self() {
      return this;
    }

    @Override
    public GcpKms build() {
      if (kmsClient == null) {
        kmsClient = buildKmsClient();
      }
      return new GcpKms(this);
    }

    private KeyManagementServiceClient buildKmsClient() {
      try {
        KeyManagementServiceSettings.Builder settings =
            KeyManagementServiceSettings.newHttpJsonBuilder();
        if (getEndpoint() != null) {
          settings.setEndpoint(getEndpoint().toString());
        }
        if (getCredentialsOverrider() != null) {
          Credentials credentials =
              GcpCredentialsProvider.getCredentials(getCredentialsOverrider());
          if (credentials != null) {
            settings.setCredentialsProvider(FixedCredentialsProvider.create(credentials));
          }
        }
        RetryConfig retryConfig = getRetryConfig();
        if (retryConfig != null) {
          settings.applyToAllUnaryMethods(
              call -> {
                call.setRetrySettings(toRetrySettings(call.getRetrySettings(), retryConfig));
                return null;
              });
        }
        return KeyManagementServiceClient.create(settings.build());
      } catch (IOException e) {
        throw new SubstrateSdkException("Could not create KeyManagementServiceClient", e);
      }
    }
  }
}
