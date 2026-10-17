package com.salesforce.multicloudj.kms.gcp;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.ApiExceptionFactory;
import com.google.api.gax.rpc.StatusCode;
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
import com.google.cloud.kms.v1.KeyManagementServiceClient.ListCryptoKeyVersionsPagedResponse;
import com.google.cloud.kms.v1.UpdateCryptoKeyPrimaryVersionRequest;
import com.google.cloud.kms.v1.UpdateCryptoKeyRequest;
import com.google.cloud.kms.v1.UpdateCryptoKeyVersionRequest;
import com.google.iam.v1.Binding;
import com.google.iam.v1.GetIamPolicyRequest;
import com.google.iam.v1.Policy;
import com.google.iam.v1.SetIamPolicyRequest;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.salesforce.multicloudj.common.exceptions.DeadlineExceededException;
import com.salesforce.multicloudj.common.exceptions.FailedPreconditionException;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceAlreadyExistsException;
import com.salesforce.multicloudj.common.exceptions.ResourceExhaustedException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.common.retries.RetryConfig;
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
import com.salesforce.multicloudj.kms.model.KeyAction;
import com.salesforce.multicloudj.kms.model.KeyGrant;
import com.salesforce.multicloudj.kms.model.KeyOrigin;
import com.salesforce.multicloudj.kms.model.KeyPolicy;
import com.salesforce.multicloudj.kms.model.KeyPrincipal;
import com.salesforce.multicloudj.kms.model.KeyRef;
import com.salesforce.multicloudj.kms.model.KeyState;
import com.salesforce.multicloudj.kms.model.PrincipalType;
import com.salesforce.multicloudj.kms.model.ProtectionLevel;
import com.salesforce.multicloudj.kms.model.WrappingAlgorithm;
import com.salesforce.multicloudj.kms.model.WrappingKeySpec;
import java.security.KeyPairGenerator;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class GcpKmsTest {

  private static final String KEY_RING = "projects/p/locations/us-west1/keyRings/ring";
  private static final String KEY_NAME = KEY_RING + "/cryptoKeys/my-key";
  private static final String VERSION_1 = KEY_NAME + "/cryptoKeyVersions/1";
  private static final String VERSION_2 = KEY_NAME + "/cryptoKeyVersions/2";
  private static final String VERSION_3 = KEY_NAME + "/cryptoKeyVersions/3";

  private KeyManagementServiceClient client;
  private List<Long> sleeps;
  private GcpKms kms;

  @BeforeEach
  void setUp() {
    client = mock(KeyManagementServiceClient.class);
    sleeps = new ArrayList<>();
    kms = newKms(KEY_RING);
  }

  private GcpKms newKms(String keyContainer) {
    return new GcpKms.Builder()
        .withKmsClient(client)
        .withKeyContainer(keyContainer)
        .withSleeper(sleeps::add)
        .build();
  }

  private static KeyRef keyRef(String keyId) {
    return KeyRef.builder().keyId(keyId).build();
  }

  private static CryptoKeyVersion version(String name, CryptoKeyVersionState state) {
    return CryptoKeyVersion.newBuilder().setName(name).setState(state).build();
  }

  private void stubVersions(CryptoKeyVersion... versions) {
    ListCryptoKeyVersionsPagedResponse paged = mock(ListCryptoKeyVersionsPagedResponse.class);
    when(paged.iterateAll()).thenReturn(List.of(versions));
    when(client.listCryptoKeyVersions(KEY_NAME)).thenReturn(paged);
  }

  // ---- identity and error mapping ----

  @Test
  void providerIdIsGcp() {
    assertEquals("gcp", kms.getProviderId());
    assertEquals("gcp", kms.builder().getProviderId());
  }

  @Test
  void closeClosesTheClient() {
    kms.close();

    verify(client).close();
  }

  @Test
  void mapExceptionClassifiesStatusCodes() {
    assertMapped(StatusCode.Code.NOT_FOUND, ResourceNotFoundException.class);
    assertMapped(StatusCode.Code.ALREADY_EXISTS, ResourceAlreadyExistsException.class);
    assertMapped(StatusCode.Code.FAILED_PRECONDITION, FailedPreconditionException.class);
    assertMapped(StatusCode.Code.INVALID_ARGUMENT, InvalidArgumentException.class);
    assertMapped(StatusCode.Code.OUT_OF_RANGE, InvalidArgumentException.class);
    assertMapped(StatusCode.Code.PERMISSION_DENIED, UnAuthorizedException.class);
    assertMapped(StatusCode.Code.UNAUTHENTICATED, UnAuthorizedException.class);
    assertMapped(StatusCode.Code.RESOURCE_EXHAUSTED, ResourceExhaustedException.class);
    assertMapped(StatusCode.Code.DEADLINE_EXCEEDED, DeadlineExceededException.class);
    assertMapped(StatusCode.Code.UNIMPLEMENTED, UnSupportedOperationException.class);
    assertMapped(StatusCode.Code.INTERNAL, UnknownException.class);
    assertMapped(StatusCode.Code.UNAVAILABLE, UnknownException.class);
  }

  @Test
  void mapExceptionPassesSdkExceptionsThroughAndWrapsOthers() {
    InvalidArgumentException sdk = new InvalidArgumentException("bad");

    assertSame(sdk, kms.mapException(sdk));
    assertEquals(UnknownException.class, kms.mapException(new IllegalStateException()).getClass());
  }

  private void assertMapped(StatusCode.Code code, Class<? extends SubstrateSdkException> expected) {
    ApiException exception = apiException(code);
    assertEquals(expected, kms.mapException(exception).getClass(), code.name());
  }

  private static ApiException apiException(StatusCode.Code code) {
    StatusCode status =
        new StatusCode() {
          @Override
          public Code getCode() {
            return code;
          }

          @Override
          public Object getTransportCode() {
            return code;
          }
        };
    return ApiExceptionFactory.createException(new RuntimeException(code.name()), status, false);
  }

  // ---- key resolution ----

  @Test
  void shortKeyIdsResolveAgainstTheKeyContainer() {
    assertEquals(KEY_NAME, kms.resolveKeyName(keyRef("my-key")));
  }

  @Test
  void fullResourceNamesAreUsedAsIs() {
    String other = "projects/q/locations/europe-west1/keyRings/r2/cryptoKeys/k";

    assertEquals(other, kms.resolveKeyName(keyRef(other)));
    assertEquals(
        other, kms.resolveKeyName(KeyRef.builder().keyId("ignored").resourceName(other).build()));
  }

  @Test
  void versionsAreDroppedFromResolvedKeyNames() {
    assertEquals(KEY_NAME, kms.resolveKeyName(keyRef(VERSION_2)));
  }

  @Test
  void shortKeyIdWithoutKeyContainerIsRejected() {
    GcpKms noContainer = newKms(null);

    assertThrows(InvalidArgumentException.class, () -> noContainer.doTagKey(keyRef("k"), Map.of()));
  }

  // ---- createKey ----

  @Test
  void createKeyBuildsASoftwareSymmetricKeyWithLabels() {
    when(client.createCryptoKey(any(CreateCryptoKeyRequest.class)))
        .thenReturn(
            CryptoKey.newBuilder()
                .setName(KEY_NAME)
                .setCreateTime(Timestamp.newBuilder().setSeconds(1_700_000_000L))
                .setPrimary(version(VERSION_1, CryptoKeyVersionState.ENABLED))
                .build());

    CreateKeyResponse response =
        kms.doCreateKey(
            CreateKeyRequest.builder()
                .keyName("my-key")
                .description("ignored")
                .tag("env", "dev")
                .build());

    ArgumentCaptor<CreateCryptoKeyRequest> captor =
        ArgumentCaptor.forClass(CreateCryptoKeyRequest.class);
    verify(client).createCryptoKey(captor.capture());
    CreateCryptoKeyRequest sent = captor.getValue();
    assertEquals(KEY_RING, sent.getParent());
    assertEquals("my-key", sent.getCryptoKeyId());
    assertFalse(sent.getSkipInitialVersionCreation());
    assertEquals(CryptoKeyPurpose.ENCRYPT_DECRYPT, sent.getCryptoKey().getPurpose());
    assertFalse(sent.getCryptoKey().getImportOnly());
    CryptoKeyVersionTemplate template = sent.getCryptoKey().getVersionTemplate();
    assertEquals(CryptoKeyVersionAlgorithm.GOOGLE_SYMMETRIC_ENCRYPTION, template.getAlgorithm());
    assertEquals(com.google.cloud.kms.v1.ProtectionLevel.SOFTWARE, template.getProtectionLevel());
    assertEquals(Map.of("env", "dev"), sent.getCryptoKey().getLabelsMap());

    assertEquals(KeyState.ENABLED, response.getKeyState());
    assertEquals(Instant.ofEpochSecond(1_700_000_000L), response.getCreationTime());
    assertNull(response.getDescription());
    KeyRef ref = response.getKeyRef();
    assertEquals("gcp", ref.getProviderId());
    assertEquals(KEY_NAME, ref.getKeyId());
    assertEquals(KEY_NAME, ref.getResourceName());
    assertEquals("us-west1", ref.getRegion());
    assertNull(ref.getVersionId());
  }

  @Test
  void createKeyWithHsmProtection() {
    when(client.createCryptoKey(any(CreateCryptoKeyRequest.class)))
        .thenReturn(CryptoKey.newBuilder().setName(KEY_NAME).build());

    kms.doCreateKey(
        CreateKeyRequest.builder().keyName("my-key").protectionLevel(ProtectionLevel.HSM).build());

    ArgumentCaptor<CreateCryptoKeyRequest> captor =
        ArgumentCaptor.forClass(CreateCryptoKeyRequest.class);
    verify(client).createCryptoKey(captor.capture());
    assertEquals(
        com.google.cloud.kms.v1.ProtectionLevel.HSM,
        captor.getValue().getCryptoKey().getVersionTemplate().getProtectionLevel());
  }

  @Test
  void createExternalKeySkipsTheInitialVersionAndIsPendingImport() {
    when(client.createCryptoKey(any(CreateCryptoKeyRequest.class)))
        .thenReturn(CryptoKey.newBuilder().setName(KEY_NAME).build());

    CreateKeyResponse response =
        kms.doCreateKey(
            CreateKeyRequest.builder().keyName("my-key").origin(KeyOrigin.EXTERNAL).build());

    ArgumentCaptor<CreateCryptoKeyRequest> captor =
        ArgumentCaptor.forClass(CreateCryptoKeyRequest.class);
    verify(client).createCryptoKey(captor.capture());
    assertTrue(captor.getValue().getSkipInitialVersionCreation());
    assertTrue(captor.getValue().getCryptoKey().getImportOnly());
    assertEquals(KeyState.PENDING_IMPORT, response.getKeyState());
  }

  @Test
  void createKeyMapsThePrimaryVersionState() {
    assertEquals(KeyState.CREATING, createdState(CryptoKeyVersionState.PENDING_GENERATION));
    assertEquals(KeyState.DISABLED, createdState(CryptoKeyVersionState.DISABLED));
    assertEquals(KeyState.PENDING_DELETION, createdState(CryptoKeyVersionState.DESTROY_SCHEDULED));
    assertEquals(KeyState.PENDING_IMPORT, createdState(CryptoKeyVersionState.PENDING_IMPORT));
    assertEquals(KeyState.UNKNOWN, createdState(CryptoKeyVersionState.DESTROYED));
  }

  private KeyState createdState(CryptoKeyVersionState primaryState) {
    when(client.createCryptoKey(any(CreateCryptoKeyRequest.class)))
        .thenReturn(
            CryptoKey.newBuilder()
                .setName(KEY_NAME)
                .setPrimary(version(VERSION_1, primaryState))
                .build());
    return kms.doCreateKey(CreateKeyRequest.builder().keyName("my-key").build()).getKeyState();
  }

  @Test
  void createKeyRequiresAName() {
    assertThrows(
        InvalidArgumentException.class, () -> kms.doCreateKey(CreateKeyRequest.builder().build()));
  }

  @Test
  void createKeyRequiresAKeyContainer() {
    GcpKms noContainer = newKms(null);

    assertThrows(
        InvalidArgumentException.class,
        () -> noContainer.doCreateKey(CreateKeyRequest.builder().keyName("k").build()));
  }

  @Test
  void createKeyRejectsMultiRegion() {
    assertThrows(
        UnSupportedOperationException.class,
        () -> kms.doCreateKey(CreateKeyRequest.builder().keyName("k").multiRegion(true).build()));
    verify(client, never()).createCryptoKey(any(CreateCryptoKeyRequest.class));
  }

  // ---- enable / disable ----

  @Test
  void enableKeyEnablesEveryDisabledVersionAndNothingElse() {
    stubVersions(
        version(VERSION_1, CryptoKeyVersionState.DISABLED),
        version(VERSION_2, CryptoKeyVersionState.ENABLED),
        version(VERSION_3, CryptoKeyVersionState.DESTROYED),
        version(KEY_NAME + "/cryptoKeyVersions/4", CryptoKeyVersionState.DISABLED));

    kms.doEnableKey(keyRef("my-key"));

    List<UpdateCryptoKeyVersionRequest> updates = capturedVersionUpdates(2);
    assertEquals(VERSION_1, updates.get(0).getCryptoKeyVersion().getName());
    assertEquals(CryptoKeyVersionState.ENABLED, updates.get(0).getCryptoKeyVersion().getState());
    assertEquals(List.of("state"), updates.get(0).getUpdateMask().getPathsList());
    assertEquals(KEY_NAME + "/cryptoKeyVersions/4", updates.get(1).getCryptoKeyVersion().getName());
  }

  @Test
  void disableKeyDisablesEveryEnabledVersionAndNothingElse() {
    stubVersions(
        version(VERSION_1, CryptoKeyVersionState.DISABLED),
        version(VERSION_2, CryptoKeyVersionState.ENABLED),
        version(VERSION_3, CryptoKeyVersionState.PENDING_GENERATION));

    kms.doDisableKey(keyRef("my-key"));

    List<UpdateCryptoKeyVersionRequest> updates = capturedVersionUpdates(1);
    assertEquals(VERSION_2, updates.get(0).getCryptoKeyVersion().getName());
    assertEquals(CryptoKeyVersionState.DISABLED, updates.get(0).getCryptoKeyVersion().getState());
  }

  @Test
  void enableKeyWithVersionTouchesOnlyThatVersion() {
    kms.doEnableKey(KeyRef.builder().keyId(KEY_NAME).versionId("2").build());

    List<UpdateCryptoKeyVersionRequest> updates = capturedVersionUpdates(1);
    assertEquals(VERSION_2, updates.get(0).getCryptoKeyVersion().getName());
    assertEquals(CryptoKeyVersionState.ENABLED, updates.get(0).getCryptoKeyVersion().getState());
    verify(client, never()).listCryptoKeyVersions(anyString());
  }

  private List<UpdateCryptoKeyVersionRequest> capturedVersionUpdates(int expected) {
    ArgumentCaptor<UpdateCryptoKeyVersionRequest> captor =
        ArgumentCaptor.forClass(UpdateCryptoKeyVersionRequest.class);
    verify(client, org.mockito.Mockito.times(expected)).updateCryptoKeyVersion(captor.capture());
    return captor.getAllValues();
  }

  // ---- import parameters ----

  private static String publicKeyPem() throws Exception {
    KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
    generator.initialize(2048);
    return pemOf(generator.generateKeyPair().getPublic().getEncoded());
  }

  private static String pemOf(byte[] der) {
    return "-----BEGIN PUBLIC KEY-----\n"
        + Base64.getMimeEncoder(64, "\n".getBytes()).encodeToString(der)
        + "\n-----END PUBLIC KEY-----\n";
  }

  private static ImportJob importJob(String name, ImportJobState state) {
    return ImportJob.newBuilder().setName(name).setState(state).build();
  }

  @Test
  void getImportParametersCreatesAnActiveJobAndReturnsTheDerKey() throws Exception {
    String pem = publicKeyPem();
    byte[] der = GcpKms.pemToDer(pem);
    String jobName = KEY_RING + "/importJobs/mcj-my-key-rsaoaep3072sha256";
    when(client.getCryptoKey(KEY_NAME))
        .thenReturn(
            CryptoKey.newBuilder()
                .setName(KEY_NAME)
                .setVersionTemplate(
                    CryptoKeyVersionTemplate.newBuilder()
                        .setProtectionLevel(com.google.cloud.kms.v1.ProtectionLevel.HSM))
                .build());
    when(client.createImportJob(any(CreateImportJobRequest.class)))
        .thenReturn(importJob(jobName, ImportJobState.PENDING_GENERATION));
    when(client.getImportJob(jobName))
        .thenReturn(
            importJob(jobName, ImportJobState.PENDING_GENERATION),
            ImportJob.newBuilder()
                .setName(jobName)
                .setState(ImportJobState.ACTIVE)
                .setPublicKey(ImportJob.WrappingPublicKey.newBuilder().setPem(pem))
                .setExpireTime(Timestamp.newBuilder().setSeconds(1_800_000_000L))
                .build());
    KeyRef requested = keyRef("my-key");

    ImportParameters parameters =
        kms.doGetImportParameters(
            GetImportParametersRequest.builder()
                .keyRef(requested)
                .wrappingAlgorithm(WrappingAlgorithm.RSAES_OAEP_SHA_256)
                .build());

    ArgumentCaptor<CreateImportJobRequest> captor =
        ArgumentCaptor.forClass(CreateImportJobRequest.class);
    verify(client).createImportJob(captor.capture());
    assertEquals(KEY_RING, captor.getValue().getParent());
    assertEquals("mcj-my-key-rsaoaep3072sha256", captor.getValue().getImportJobId());
    assertEquals(
        ImportMethod.RSA_OAEP_3072_SHA256, captor.getValue().getImportJob().getImportMethod());
    assertEquals(
        com.google.cloud.kms.v1.ProtectionLevel.HSM,
        captor.getValue().getImportJob().getProtectionLevel());

    assertEquals(List.of(1000L, 1000L), sleeps);
    assertArrayEquals(der, parameters.getWrappingPublicKey());
    assertEquals(jobName, parameters.getImportToken());
    assertEquals(WrappingAlgorithm.RSAES_OAEP_SHA_256, parameters.getWrappingAlgorithm());
    assertEquals(WrappingKeySpec.RSA_3072, parameters.getWrappingKeySpec());
    assertEquals(Instant.ofEpochSecond(1_800_000_000L), parameters.getTokenExpiry());
    assertSame(requested, parameters.getKeyRef());
  }

  @Test
  void getImportParametersReusesAnExistingActiveJob() throws Exception {
    String jobName = KEY_RING + "/importJobs/mcj-my-key-rsaoaep4096sha256aes256";
    when(client.getCryptoKey(KEY_NAME))
        .thenReturn(CryptoKey.newBuilder().setName(KEY_NAME).build());
    when(client.createImportJob(any(CreateImportJobRequest.class)))
        .thenThrow((AlreadyExistsException) apiException(StatusCode.Code.ALREADY_EXISTS));
    when(client.getImportJob(jobName))
        .thenReturn(
            ImportJob.newBuilder()
                .setName(jobName)
                .setState(ImportJobState.ACTIVE)
                .setPublicKey(ImportJob.WrappingPublicKey.newBuilder().setPem(publicKeyPem()))
                .build());

    ImportParameters parameters =
        kms.doGetImportParameters(
            GetImportParametersRequest.builder()
                .keyRef(keyRef("my-key"))
                .wrappingAlgorithm(WrappingAlgorithm.RSA_AES_KEY_WRAP_SHA_256)
                .wrappingKeySpec(WrappingKeySpec.RSA_4096)
                .build());

    ArgumentCaptor<CreateImportJobRequest> captor =
        ArgumentCaptor.forClass(CreateImportJobRequest.class);
    verify(client).createImportJob(captor.capture());
    assertEquals(
        ImportMethod.RSA_OAEP_4096_SHA256_AES_256,
        captor.getValue().getImportJob().getImportMethod());
    assertEquals(
        com.google.cloud.kms.v1.ProtectionLevel.SOFTWARE,
        captor.getValue().getImportJob().getProtectionLevel());
    assertEquals(jobName, parameters.getImportToken());
    assertEquals(WrappingKeySpec.RSA_4096, parameters.getWrappingKeySpec());
    assertTrue(sleeps.isEmpty());
  }

  @Test
  void getImportParametersRejectsAnExpiredExistingJob() {
    String jobName = KEY_RING + "/importJobs/mcj-my-key-rsaoaep3072sha256";
    when(client.getCryptoKey(KEY_NAME))
        .thenReturn(CryptoKey.newBuilder().setName(KEY_NAME).build());
    when(client.createImportJob(any(CreateImportJobRequest.class)))
        .thenThrow((AlreadyExistsException) apiException(StatusCode.Code.ALREADY_EXISTS));
    when(client.getImportJob(jobName)).thenReturn(importJob(jobName, ImportJobState.EXPIRED));

    assertThrows(
        FailedPreconditionException.class,
        () ->
            kms.doGetImportParameters(
                GetImportParametersRequest.builder().keyRef(keyRef("my-key")).build()));
  }

  @Test
  void getImportParametersGivesUpWhenTheJobNeverBecomesActive() {
    String jobName = KEY_RING + "/importJobs/mcj-my-key-rsaoaep3072sha256";
    when(client.getCryptoKey(KEY_NAME))
        .thenReturn(CryptoKey.newBuilder().setName(KEY_NAME).build());
    ImportJob pending = importJob(jobName, ImportJobState.PENDING_GENERATION);
    when(client.createImportJob(any(CreateImportJobRequest.class))).thenReturn(pending);
    when(client.getImportJob(jobName)).thenReturn(pending);

    assertThrows(
        DeadlineExceededException.class,
        () ->
            kms.doGetImportParameters(
                GetImportParametersRequest.builder().keyRef(keyRef("my-key")).build()));
    assertEquals(120, sleeps.size());
  }

  @Test
  void getImportParametersRejectsRsa2048BeforeCallingTheService() {
    assertThrows(
        UnSupportedOperationException.class,
        () ->
            kms.doGetImportParameters(
                GetImportParametersRequest.builder()
                    .keyRef(keyRef("my-key"))
                    .wrappingKeySpec(WrappingKeySpec.RSA_2048)
                    .build()));
    verify(client, never()).getCryptoKey(anyString());
  }

  @Test
  void importMethodFollowsAlgorithmAndKeySize() {
    assertEquals(
        ImportMethod.RSA_OAEP_3072_SHA256,
        GcpKms.toImportMethod(WrappingAlgorithm.RSAES_OAEP_SHA_256, WrappingKeySpec.RSA_3072));
    assertEquals(
        ImportMethod.RSA_OAEP_4096_SHA256,
        GcpKms.toImportMethod(WrappingAlgorithm.RSAES_OAEP_SHA_256, WrappingKeySpec.RSA_4096));
    assertEquals(
        ImportMethod.RSA_OAEP_3072_SHA256_AES_256,
        GcpKms.toImportMethod(
            WrappingAlgorithm.RSA_AES_KEY_WRAP_SHA_256, WrappingKeySpec.RSA_3072));
    assertEquals(
        ImportMethod.RSA_OAEP_4096_SHA256_AES_256,
        GcpKms.toImportMethod(
            WrappingAlgorithm.RSA_AES_KEY_WRAP_SHA_256, WrappingKeySpec.RSA_4096));
  }

  @Test
  void importJobIdIsDeterministicAndFitsTheIdLimit() {
    String id = GcpKms.importJobId("my-key", ImportMethod.RSA_OAEP_3072_SHA256);
    String longKey = "k".repeat(100);
    String longId = GcpKms.importJobId(longKey, ImportMethod.RSA_OAEP_4096_SHA256_AES_256);

    assertEquals("mcj-my-key-rsaoaep3072sha256", id);
    assertEquals(id, GcpKms.importJobId("my-key", ImportMethod.RSA_OAEP_3072_SHA256));
    assertEquals(63, longId.length());
    assertEquals(longId, GcpKms.importJobId(longKey, ImportMethod.RSA_OAEP_4096_SHA256_AES_256));
    assertFalse(
        longId.equals(
            GcpKms.importJobId(
                "k".repeat(99) + "x" + "k", ImportMethod.RSA_OAEP_4096_SHA256_AES_256)));
  }

  @Test
  void pemIsDecodedToDer() {
    byte[] der = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

    assertArrayEquals(der, GcpKms.pemToDer(pemOf(der)));
  }

  // ---- import key material ----

  private static ImportKeyMaterialRequest importRequest(String token) {
    return ImportKeyMaterialRequest.builder()
        .keyRef(keyRef("my-key"))
        .importToken(token)
        .wrappedKeyMaterial(new byte[] {9, 8, 7})
        .build();
  }

  @Test
  void importKeyMaterialRejectsExpiringMaterial() {
    ImportKeyMaterialRequest request =
        ImportKeyMaterialRequest.builder()
            .keyRef(keyRef("my-key"))
            .importToken("job")
            .wrappedKeyMaterial(new byte[] {1})
            .expirationModel(ExpirationModel.KEY_MATERIAL_EXPIRES)
            .validTo(Instant.now())
            .build();

    assertThrows(UnSupportedOperationException.class, () -> kms.doImportKeyMaterial(request));
    verify(client, never()).importCryptoKeyVersion(any(ImportCryptoKeyVersionRequest.class));
  }

  @Test
  void importKeyMaterialWaitsForTheVersionAndSetsThePrimaryWhenMissing() {
    String token = KEY_RING + "/importJobs/job";
    when(client.getImportJob(token))
        .thenReturn(
            ImportJob.newBuilder().setImportMethod(ImportMethod.RSA_OAEP_3072_SHA256).build());
    when(client.importCryptoKeyVersion(any(ImportCryptoKeyVersionRequest.class)))
        .thenReturn(version(VERSION_1, CryptoKeyVersionState.PENDING_IMPORT));
    when(client.getCryptoKeyVersion(VERSION_1))
        .thenReturn(version(VERSION_1, CryptoKeyVersionState.ENABLED));
    when(client.getCryptoKey(KEY_NAME))
        .thenReturn(CryptoKey.newBuilder().setName(KEY_NAME).build());

    KeyRef imported = kms.doImportKeyMaterial(importRequest(token));

    ArgumentCaptor<ImportCryptoKeyVersionRequest> captor =
        ArgumentCaptor.forClass(ImportCryptoKeyVersionRequest.class);
    verify(client).importCryptoKeyVersion(captor.capture());
    ImportCryptoKeyVersionRequest sent = captor.getValue();
    assertEquals(KEY_NAME, sent.getParent());
    assertEquals(CryptoKeyVersionAlgorithm.GOOGLE_SYMMETRIC_ENCRYPTION, sent.getAlgorithm());
    assertEquals(token, sent.getImportJob());
    assertEquals(ByteString.copyFrom(new byte[] {9, 8, 7}), sent.getWrappedKey());
    assertTrue(sent.getRsaAesWrappedKey().isEmpty());
    assertEquals(List.of(1000L), sleeps);

    ArgumentCaptor<UpdateCryptoKeyPrimaryVersionRequest> primary =
        ArgumentCaptor.forClass(UpdateCryptoKeyPrimaryVersionRequest.class);
    verify(client).updateCryptoKeyPrimaryVersion(primary.capture());
    assertEquals(KEY_NAME, primary.getValue().getName());
    assertEquals("1", primary.getValue().getCryptoKeyVersionId());

    assertEquals(KEY_NAME, imported.getKeyId());
    assertEquals("1", imported.getVersionId());
    assertEquals("gcp", imported.getProviderId());
  }

  @Test
  void importKeyMaterialUsesTheAesWrappedFieldForAesImportMethods() {
    String token = KEY_RING + "/importJobs/job";
    when(client.getImportJob(token))
        .thenReturn(
            ImportJob.newBuilder()
                .setImportMethod(ImportMethod.RSA_OAEP_4096_SHA256_AES_256)
                .build());
    when(client.importCryptoKeyVersion(any(ImportCryptoKeyVersionRequest.class)))
        .thenReturn(version(VERSION_1, CryptoKeyVersionState.ENABLED));
    when(client.getCryptoKey(KEY_NAME))
        .thenReturn(
            CryptoKey.newBuilder()
                .setName(KEY_NAME)
                .setPrimary(version(VERSION_1, CryptoKeyVersionState.ENABLED))
                .build());

    kms.doImportKeyMaterial(importRequest(token));

    ArgumentCaptor<ImportCryptoKeyVersionRequest> captor =
        ArgumentCaptor.forClass(ImportCryptoKeyVersionRequest.class);
    verify(client).importCryptoKeyVersion(captor.capture());
    assertEquals(
        ByteString.copyFrom(new byte[] {9, 8, 7}), captor.getValue().getRsaAesWrappedKey());
    assertTrue(captor.getValue().getWrappedKey().isEmpty());
    verify(client, never())
        .updateCryptoKeyPrimaryVersion(any(UpdateCryptoKeyPrimaryVersionRequest.class));
  }

  @Test
  void importKeyMaterialReportsAFailedImport() {
    String token = KEY_RING + "/importJobs/job";
    when(client.getImportJob(token)).thenReturn(ImportJob.getDefaultInstance());
    when(client.importCryptoKeyVersion(any(ImportCryptoKeyVersionRequest.class)))
        .thenReturn(version(VERSION_1, CryptoKeyVersionState.PENDING_IMPORT));
    when(client.getCryptoKeyVersion(VERSION_1))
        .thenReturn(
            CryptoKeyVersion.newBuilder()
                .setName(VERSION_1)
                .setState(CryptoKeyVersionState.IMPORT_FAILED)
                .setImportFailureReason("bad wrapping")
                .build());

    FailedPreconditionException e =
        assertThrows(
            FailedPreconditionException.class, () -> kms.doImportKeyMaterial(importRequest(token)));

    assertTrue(e.getMessage().contains("bad wrapping"));
    verify(client, never()).getCryptoKey(anyString());
  }

  // ---- policy ----

  @Test
  void setKeyPolicyReplacesTheBindingsUsingTheCurrentEtag() {
    when(client.getIamPolicy(any(GetIamPolicyRequest.class)))
        .thenReturn(
            Policy.newBuilder()
                .setEtag(ByteString.copyFromUtf8("etag-1"))
                .addBindings(Binding.newBuilder().setRole("roles/owner").addMembers("user:x"))
                .build());
    KeyPolicy policy =
        KeyPolicy.builder()
            .grant(
                KeyGrant.builder()
                    .principal(
                        KeyPrincipal.of(PrincipalType.IAM_ROLE, "sa@p.iam.gserviceaccount.com"))
                    .action(KeyAction.ENCRYPT)
                    .build())
            .build();

    kms.doSetKeyPolicy(keyRef("my-key"), policy);

    ArgumentCaptor<GetIamPolicyRequest> read = ArgumentCaptor.forClass(GetIamPolicyRequest.class);
    verify(client).getIamPolicy(read.capture());
    assertEquals(KEY_NAME, read.getValue().getResource());
    ArgumentCaptor<SetIamPolicyRequest> write = ArgumentCaptor.forClass(SetIamPolicyRequest.class);
    verify(client).setIamPolicy(write.capture());
    assertEquals(KEY_NAME, write.getValue().getResource());
    Policy sent = write.getValue().getPolicy();
    assertEquals(ByteString.copyFromUtf8("etag-1"), sent.getEtag());
    assertEquals(1, sent.getBindingsCount());
    assertEquals("roles/cloudkms.cryptoKeyEncrypter", sent.getBindings(0).getRole());
    assertEquals(
        List.of("serviceAccount:sa@p.iam.gserviceaccount.com"),
        sent.getBindings(0).getMembersList());
  }

  @Test
  void getKeyPolicyTranslatesTheStoredPolicy() {
    when(client.getIamPolicy(any(GetIamPolicyRequest.class)))
        .thenReturn(
            Policy.newBuilder()
                .addBindings(
                    Binding.newBuilder()
                        .setRole("roles/cloudkms.viewer")
                        .addMembers("serviceAccount:sa@p.iam.gserviceaccount.com"))
                .build());

    KeyPolicy policy = kms.doGetKeyPolicy(keyRef(KEY_NAME));

    ArgumentCaptor<GetIamPolicyRequest> read = ArgumentCaptor.forClass(GetIamPolicyRequest.class);
    verify(client).getIamPolicy(read.capture());
    assertEquals(KEY_NAME, read.getValue().getResource());
    assertEquals(1, policy.getGrants().size());
    assertEquals(
        KeyPrincipal.of(PrincipalType.IAM_ROLE, "sa@p.iam.gserviceaccount.com"),
        policy.getGrants().get(0).getPrincipals().get(0));
  }

  @Test
  void policyCapabilitiesComeFromTheTranslator() {
    assertFalse(kms.getPolicyCapabilities().isSupportsDeny());
    assertFalse(kms.getPolicyCapabilities().isSupportsConditions());
  }

  // ---- tags ----

  @Test
  void tagKeyMergesNewLabelsOverExistingOnes() {
    when(client.getCryptoKey(KEY_NAME))
        .thenReturn(
            CryptoKey.newBuilder()
                .setName(KEY_NAME)
                .putLabels("keep", "1")
                .putLabels("env", "old")
                .build());

    kms.doTagKey(keyRef("my-key"), Map.of("env", "new", "added", "2"));

    ArgumentCaptor<UpdateCryptoKeyRequest> captor =
        ArgumentCaptor.forClass(UpdateCryptoKeyRequest.class);
    verify(client).updateCryptoKey(captor.capture());
    assertEquals(KEY_NAME, captor.getValue().getCryptoKey().getName());
    assertEquals(
        Map.of("keep", "1", "env", "new", "added", "2"),
        captor.getValue().getCryptoKey().getLabelsMap());
    assertEquals(List.of("labels"), captor.getValue().getUpdateMask().getPathsList());
  }

  // ---- encrypt / decrypt ----

  @Test
  void encryptTargetsTheKeyAndBindsTheEncryptionContext() {
    when(client.encrypt(any(com.google.cloud.kms.v1.EncryptRequest.class)))
        .thenReturn(
            com.google.cloud.kms.v1.EncryptResponse.newBuilder()
                .setName(VERSION_2)
                .setCiphertext(ByteString.copyFromUtf8("cipher"))
                .build());

    EncryptResponse response =
        kms.doEncrypt(
            EncryptRequest.builder()
                .keyRef(keyRef("my-key"))
                .plaintext("plain".getBytes())
                .encryptionContextEntry("b", "2")
                .encryptionContextEntry("a", "1")
                .build());

    ArgumentCaptor<com.google.cloud.kms.v1.EncryptRequest> captor =
        ArgumentCaptor.forClass(com.google.cloud.kms.v1.EncryptRequest.class);
    verify(client).encrypt(captor.capture());
    assertEquals(KEY_NAME, captor.getValue().getName());
    assertEquals(ByteString.copyFromUtf8("plain"), captor.getValue().getPlaintext());
    assertEquals(
        ByteString.copyFromUtf8("{\"a\":\"1\",\"b\":\"2\"}"),
        captor.getValue().getAdditionalAuthenticatedData());
    assertArrayEquals("cipher".getBytes(), response.getCiphertext());
    assertEquals(KEY_NAME, response.getKeyRef().getKeyId());
    assertEquals("2", response.getKeyRef().getVersionId());
  }

  @Test
  void encryptWithAVersionTargetsThatVersionAndOmitsEmptyContext() {
    when(client.encrypt(any(com.google.cloud.kms.v1.EncryptRequest.class)))
        .thenReturn(
            com.google.cloud.kms.v1.EncryptResponse.newBuilder()
                .setName(VERSION_3)
                .setCiphertext(ByteString.copyFromUtf8("c"))
                .build());

    kms.doEncrypt(
        EncryptRequest.builder()
            .keyRef(KeyRef.builder().keyId(KEY_NAME).versionId("3").build())
            .plaintext(new byte[] {1})
            .build());

    ArgumentCaptor<com.google.cloud.kms.v1.EncryptRequest> captor =
        ArgumentCaptor.forClass(com.google.cloud.kms.v1.EncryptRequest.class);
    verify(client).encrypt(captor.capture());
    assertEquals(VERSION_3, captor.getValue().getName());
    assertTrue(captor.getValue().getAdditionalAuthenticatedData().isEmpty());
  }

  @Test
  void decryptTargetsTheKeyWithoutAVersionAndRebuildsTheSameContext() {
    when(client.decrypt(any(com.google.cloud.kms.v1.DecryptRequest.class)))
        .thenReturn(
            com.google.cloud.kms.v1.DecryptResponse.newBuilder()
                .setPlaintext(ByteString.copyFromUtf8("plain"))
                .build());

    DecryptResponse response =
        kms.doDecrypt(
            DecryptRequest.builder()
                .keyRef(KeyRef.builder().keyId(KEY_NAME).versionId("2").build())
                .ciphertext(new byte[] {5})
                .encryptionContextEntry("a", "1")
                .encryptionContextEntry("b", "2")
                .build());

    ArgumentCaptor<com.google.cloud.kms.v1.DecryptRequest> captor =
        ArgumentCaptor.forClass(com.google.cloud.kms.v1.DecryptRequest.class);
    verify(client).decrypt(captor.capture());
    assertEquals(KEY_NAME, captor.getValue().getName());
    assertEquals(ByteString.copyFrom(new byte[] {5}), captor.getValue().getCiphertext());
    assertEquals(
        ByteString.copyFromUtf8("{\"a\":\"1\",\"b\":\"2\"}"),
        captor.getValue().getAdditionalAuthenticatedData());
    assertArrayEquals("plain".getBytes(), response.getPlaintext());
  }

  @Test
  void decryptRequiresAKeyRef() {
    assertThrows(
        InvalidArgumentException.class,
        () -> kms.doDecrypt(DecryptRequest.builder().ciphertext(new byte[] {1}).build()));
  }

  @Test
  void encryptionContextEncodingIgnoresInsertionOrder() {
    Map<String, String> first = new LinkedHashMap<>();
    first.put("z", "1");
    first.put("a", "2");
    Map<String, String> second = new LinkedHashMap<>();
    second.put("a", "2");
    second.put("z", "1");

    assertEquals(GcpKms.encodeEncryptionContext(first), GcpKms.encodeEncryptionContext(second));
    assertEquals(
        ByteString.copyFromUtf8("{\"a\":\"2\",\"z\":\"1\"}"),
        GcpKms.encodeEncryptionContext(first));
    assertTrue(GcpKms.encodeEncryptionContext(Map.of()).isEmpty());
    assertTrue(GcpKms.encodeEncryptionContext(null).isEmpty());
  }

  // ---- retry configuration ----

  @Test
  void retryConfigOverridesTheBaseRetrySettings() {
    RetrySettings base =
        RetrySettings.newBuilder()
            .setMaxAttempts(2)
            .setInitialRetryDelayDuration(Duration.ofMillis(5))
            .setMaxRetryDelayDuration(Duration.ofMillis(50))
            .setRetryDelayMultiplier(1.5)
            .setTotalTimeoutDuration(Duration.ofSeconds(30))
            .build();

    RetrySettings fixed =
        GcpKms.toRetrySettings(
            base,
            RetryConfig.builder()
                .mode(RetryConfig.Mode.FIXED)
                .maxAttempts(4)
                .fixedDelayMillis(200)
                .totalTimeout(10_000L)
                .attemptTimeout(2_000L)
                .build());

    assertEquals(4, fixed.getMaxAttempts());
    assertEquals(Duration.ofMillis(200), fixed.getInitialRetryDelayDuration());
    assertEquals(Duration.ofMillis(200), fixed.getMaxRetryDelayDuration());
    assertEquals(1.0, fixed.getRetryDelayMultiplier());
    assertEquals(Duration.ofSeconds(10), fixed.getTotalTimeoutDuration());
    assertEquals(Duration.ofSeconds(2), fixed.getInitialRpcTimeoutDuration());
    assertEquals(Duration.ofSeconds(2), fixed.getMaxRpcTimeoutDuration());

    RetrySettings exponential =
        GcpKms.toRetrySettings(
            base,
            RetryConfig.builder()
                .mode(RetryConfig.Mode.EXPONENTIAL)
                .initialDelayMillis(100)
                .multiplier(3.0)
                .maxDelayMillis(900)
                .build());

    assertEquals(2, exponential.getMaxAttempts());
    assertEquals(Duration.ofMillis(100), exponential.getInitialRetryDelayDuration());
    assertEquals(3.0, exponential.getRetryDelayMultiplier());
    assertEquals(Duration.ofMillis(900), exponential.getMaxRetryDelayDuration());
    assertEquals(Duration.ofSeconds(30), exponential.getTotalTimeoutDuration());
  }
}
