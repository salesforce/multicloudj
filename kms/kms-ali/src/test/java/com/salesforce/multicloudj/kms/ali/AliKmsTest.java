package com.salesforce.multicloudj.kms.ali;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.aliyun.kms20160120.Client;
import com.aliyun.kms20160120.models.CreateAliasRequest;
import com.aliyun.kms20160120.models.CreateAliasResponse;
import com.aliyun.kms20160120.models.CreateKeyRequest;
import com.aliyun.kms20160120.models.CreateKeyResponse;
import com.aliyun.kms20160120.models.CreateKeyResponseBody;
import com.aliyun.kms20160120.models.CreateKeyResponseBody.CreateKeyResponseBodyKeyMetadata;
import com.aliyun.kms20160120.models.DecryptRequest;
import com.aliyun.kms20160120.models.DecryptResponse;
import com.aliyun.kms20160120.models.DecryptResponseBody;
import com.aliyun.kms20160120.models.DisableKeyRequest;
import com.aliyun.kms20160120.models.EnableKeyRequest;
import com.aliyun.kms20160120.models.EncryptRequest;
import com.aliyun.kms20160120.models.EncryptResponse;
import com.aliyun.kms20160120.models.EncryptResponseBody;
import com.aliyun.kms20160120.models.GetKeyPolicyRequest;
import com.aliyun.kms20160120.models.GetKeyPolicyResponse;
import com.aliyun.kms20160120.models.GetKeyPolicyResponseBody;
import com.aliyun.kms20160120.models.GetParametersForImportRequest;
import com.aliyun.kms20160120.models.GetParametersForImportResponse;
import com.aliyun.kms20160120.models.GetParametersForImportResponseBody;
import com.aliyun.kms20160120.models.ImportKeyMaterialRequest;
import com.aliyun.kms20160120.models.SetKeyPolicyRequest;
import com.aliyun.kms20160120.models.TagResourceRequest;
import com.aliyun.tea.TeaException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.kms.client.KmsClient;
import com.salesforce.multicloudj.kms.model.ExpirationModel;
import com.salesforce.multicloudj.kms.model.GetImportParametersRequest;
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
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class AliKmsTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String REGION = "cn-shanghai";
  private static final KeyRef KEY = KeyRef.builder().keyId("key-1").build();

  private Client client;
  private AliKms kms;

  @BeforeEach
  void setUp() {
    client = mock(Client.class);
    kms = new AliKms.Builder().withRegion(REGION).withKmsClient(client).build();
  }

  private static CreateKeyResponse createKeyResponse(String state) {
    return new CreateKeyResponse()
        .setBody(
            new CreateKeyResponseBody()
                .setKeyMetadata(
                    new CreateKeyResponseBodyKeyMetadata()
                        .setKeyId("key-1")
                        .setArn("acs:kms:cn-shanghai:1:key/key-1")
                        .setKeyState(state)
                        .setCreationDate("2024-05-01T10:15:30Z")
                        .setDescription("desc")));
  }

  @Test
  void providerIdAndBuilder() {
    assertEquals("ali", kms.getProviderId());
    assertEquals(AliKms.Builder.class, kms.builder().getClass());
    assertEquals("ali", new AliKms().getProviderId());
  }

  @Test
  void createKeyMapsRequestAndResponse() throws Exception {
    when(client.createKey(any())).thenReturn(createKeyResponse("Enabled"));
    AliKms scoped =
        new AliKms.Builder()
            .withRegion(REGION)
            .withKeyContainer("kst-instance")
            .withKmsClient(client)
            .build();

    com.salesforce.multicloudj.kms.model.CreateKeyResponse response =
        scoped.createKey(
            com.salesforce.multicloudj.kms.model.CreateKeyRequest.builder()
                .description("desc")
                .protectionLevel(ProtectionLevel.HSM)
                .tag("env", "test")
                .build());

    ArgumentCaptor<CreateKeyRequest> captor = ArgumentCaptor.forClass(CreateKeyRequest.class);
    verify(client).createKey(captor.capture());
    CreateKeyRequest sent = captor.getValue();
    assertEquals("Aliyun_AES_256", sent.getKeySpec());
    assertEquals("ENCRYPT/DECRYPT", sent.getKeyUsage());
    assertEquals("Aliyun_KMS", sent.getOrigin());
    assertEquals("HSM", sent.getProtectionLevel());
    assertEquals("desc", sent.getDescription());
    assertEquals("kst-instance", sent.getDKMSInstanceId());
    JsonNode tags = MAPPER.readTree(sent.getTags());
    assertEquals("env", tags.get(0).get("TagKey").asText());
    assertEquals("test", tags.get(0).get("TagValue").asText());
    verify(client, never()).createAlias(any());

    assertEquals("ali", response.getKeyRef().getProviderId());
    assertEquals(REGION, response.getKeyRef().getRegion());
    assertEquals("key-1", response.getKeyRef().getKeyId());
    assertEquals("acs:kms:cn-shanghai:1:key/key-1", response.getKeyRef().getResourceName());
    assertEquals(KeyState.ENABLED, response.getKeyState());
    assertEquals(Instant.parse("2024-05-01T10:15:30Z"), response.getCreationTime());
    assertEquals("desc", response.getDescription());
  }

  @Test
  void createKeyDefaultsOmitOptionalFields() throws Exception {
    when(client.createKey(any())).thenReturn(createKeyResponse("PendingImport"));

    com.salesforce.multicloudj.kms.model.CreateKeyResponse response =
        kms.createKey(
            com.salesforce.multicloudj.kms.model.CreateKeyRequest.builder()
                .origin(KeyOrigin.EXTERNAL)
                .build());

    ArgumentCaptor<CreateKeyRequest> captor = ArgumentCaptor.forClass(CreateKeyRequest.class);
    verify(client).createKey(captor.capture());
    assertEquals("EXTERNAL", captor.getValue().getOrigin());
    assertEquals("SOFTWARE", captor.getValue().getProtectionLevel());
    assertNull(captor.getValue().getDKMSInstanceId());
    assertNull(captor.getValue().getTags());
    assertNull(captor.getValue().getDescription());
    assertEquals(KeyState.PENDING_IMPORT, response.getKeyState());
  }

  @Test
  void createKeyRegistersAliasForKeyName() throws Exception {
    when(client.createKey(any())).thenReturn(createKeyResponse("Enabled"));
    when(client.createAlias(any())).thenReturn(new CreateAliasResponse());

    kms.createKey(
        com.salesforce.multicloudj.kms.model.CreateKeyRequest.builder().keyName("my-key").build());

    ArgumentCaptor<CreateAliasRequest> captor = ArgumentCaptor.forClass(CreateAliasRequest.class);
    verify(client).createAlias(captor.capture());
    assertEquals("alias/my-key", captor.getValue().getAliasName());
    assertEquals("key-1", captor.getValue().getKeyId());
  }

  @Test
  void createKeyMapsKeyStates() throws Exception {
    Map<String, KeyState> expected =
        Map.of(
            "Disabled", KeyState.DISABLED,
            "PendingDeletion", KeyState.PENDING_DELETION,
            "Mystery", KeyState.UNKNOWN);
    for (Map.Entry<String, KeyState> entry : expected.entrySet()) {
      when(client.createKey(any())).thenReturn(createKeyResponse(entry.getKey()));
      assertEquals(
          entry.getValue(),
          kms.createKey(com.salesforce.multicloudj.kms.model.CreateKeyRequest.builder().build())
              .getKeyState());
    }
  }

  @Test
  void createKeyRejectsMultiRegion() {
    assertThrows(
        UnSupportedOperationException.class,
        () ->
            kms.createKey(
                com.salesforce.multicloudj.kms.model.CreateKeyRequest.builder()
                    .multiRegion(true)
                    .build()));
    verifyNoInteractions(client);
  }

  @Test
  void enableAndDisableUseKeyId() throws Exception {
    kms.enableKey(KEY);
    kms.disableKey(KEY);

    ArgumentCaptor<EnableKeyRequest> enable = ArgumentCaptor.forClass(EnableKeyRequest.class);
    ArgumentCaptor<DisableKeyRequest> disable = ArgumentCaptor.forClass(DisableKeyRequest.class);
    verify(client).enableKey(enable.capture());
    verify(client).disableKey(disable.capture());
    assertEquals("key-1", enable.getValue().getKeyId());
    assertEquals("key-1", disable.getValue().getKeyId());
  }

  @Test
  void enableAndDisableRejectVersionId() {
    KeyRef versioned = KEY.toBuilder().versionId("v1").build();

    assertThrows(UnSupportedOperationException.class, () -> kms.enableKey(versioned));
    assertThrows(UnSupportedOperationException.class, () -> kms.disableKey(versioned));
    verifyNoInteractions(client);
  }

  @Test
  void getImportParametersDefaultsToRsa2048AndDecodesPublicKey() throws Exception {
    byte[] der = {1, 2, 3, 4};
    when(client.getParametersForImport(any()))
        .thenReturn(
            new GetParametersForImportResponse()
                .setBody(
                    new GetParametersForImportResponseBody()
                        .setPublicKey(Base64.getEncoder().encodeToString(der))
                        .setImportToken("token")
                        .setTokenExpireTime("2030-01-02T03:04:05Z")));

    ImportParameters parameters =
        kms.getImportParameters(GetImportParametersRequest.builder().keyRef(KEY).build());

    ArgumentCaptor<GetParametersForImportRequest> captor =
        ArgumentCaptor.forClass(GetParametersForImportRequest.class);
    verify(client).getParametersForImport(captor.capture());
    assertEquals("key-1", captor.getValue().getKeyId());
    assertEquals("RSAES_OAEP_SHA_256", captor.getValue().getWrappingAlgorithm());
    assertEquals("RSA_2048", captor.getValue().getWrappingKeySpec());
    assertArrayEquals(der, parameters.getWrappingPublicKey());
    assertEquals("token", parameters.getImportToken());
    assertEquals(Instant.parse("2030-01-02T03:04:05Z"), parameters.getTokenExpiry());
    assertEquals(WrappingAlgorithm.RSAES_OAEP_SHA_256, parameters.getWrappingAlgorithm());
    assertEquals(WrappingKeySpec.RSA_2048, parameters.getWrappingKeySpec());
    assertSame(KEY, parameters.getKeyRef());
  }

  @Test
  void getImportParametersHonorsRequestedKeySpec() throws Exception {
    when(client.getParametersForImport(any()))
        .thenReturn(
            new GetParametersForImportResponse()
                .setBody(
                    new GetParametersForImportResponseBody()
                        .setPublicKey(Base64.getEncoder().encodeToString(new byte[] {1}))
                        .setImportToken("t")));

    ImportParameters parameters =
        kms.getImportParameters(
            GetImportParametersRequest.builder()
                .keyRef(KEY)
                .wrappingKeySpec(WrappingKeySpec.RSA_4096)
                .build());

    ArgumentCaptor<GetParametersForImportRequest> captor =
        ArgumentCaptor.forClass(GetParametersForImportRequest.class);
    verify(client).getParametersForImport(captor.capture());
    assertEquals("RSA_4096", captor.getValue().getWrappingKeySpec());
    assertEquals(WrappingKeySpec.RSA_4096, parameters.getWrappingKeySpec());
    assertNull(parameters.getTokenExpiry());
  }

  @Test
  void getImportParametersRejectsAesKeyWrap() {
    assertThrows(
        UnSupportedOperationException.class,
        () ->
            kms.getImportParameters(
                GetImportParametersRequest.builder()
                    .keyRef(KEY)
                    .wrappingAlgorithm(WrappingAlgorithm.RSA_AES_KEY_WRAP_SHA_256)
                    .build()));
    verifyNoInteractions(client);
  }

  @Test
  void importKeyMaterialEncodesMaterialAndExpiry() throws Exception {
    byte[] wrapped = {9, 8, 7};
    Instant validTo = Instant.ofEpochSecond(1_900_000_000L);

    KeyRef result =
        kms.importKeyMaterial(
            com.salesforce.multicloudj.kms.model.ImportKeyMaterialRequest.builder()
                .keyRef(KEY)
                .importToken("token")
                .wrappedKeyMaterial(wrapped)
                .expirationModel(ExpirationModel.KEY_MATERIAL_EXPIRES)
                .validTo(validTo)
                .build());

    ArgumentCaptor<ImportKeyMaterialRequest> captor =
        ArgumentCaptor.forClass(ImportKeyMaterialRequest.class);
    verify(client).importKeyMaterial(captor.capture());
    assertEquals("key-1", captor.getValue().getKeyId());
    assertEquals("token", captor.getValue().getImportToken());
    assertEquals(
        Base64.getEncoder().encodeToString(wrapped), captor.getValue().getEncryptedKeyMaterial());
    assertEquals(1_900_000_000L, captor.getValue().getKeyMaterialExpireUnix());
    assertSame(KEY, result);
  }

  @Test
  void importKeyMaterialWithoutExpiryUsesZero() throws Exception {
    kms.importKeyMaterial(
        com.salesforce.multicloudj.kms.model.ImportKeyMaterialRequest.builder()
            .keyRef(KEY)
            .importToken("token")
            .wrappedKeyMaterial(new byte[] {1})
            .build());

    ArgumentCaptor<ImportKeyMaterialRequest> captor =
        ArgumentCaptor.forClass(ImportKeyMaterialRequest.class);
    verify(client).importKeyMaterial(captor.capture());
    assertEquals(0L, captor.getValue().getKeyMaterialExpireUnix());
  }

  @Test
  void setAndGetKeyPolicyRoundTripThroughNativeDocument() throws Exception {
    KeyPolicy policy =
        KeyPolicy.builder()
            .grant(
                KeyGrant.builder()
                    .principal(KeyPrincipal.of(PrincipalType.IAM_ROLE, "acs:ram::1:role/a"))
                    .action(KeyAction.ENCRYPT)
                    .action(KeyAction.DECRYPT)
                    .build())
            .build();

    kms.setKeyPolicy(KEY, policy);

    ArgumentCaptor<SetKeyPolicyRequest> captor = ArgumentCaptor.forClass(SetKeyPolicyRequest.class);
    verify(client).setKeyPolicy(captor.capture());
    assertEquals("key-1", captor.getValue().getKeyId());
    assertEquals("default", captor.getValue().getPolicyName());

    when(client.getKeyPolicy(any()))
        .thenReturn(
            new GetKeyPolicyResponse()
                .setBody(new GetKeyPolicyResponseBody().setPolicy(captor.getValue().getPolicy())));
    assertEquals(policy, kms.getKeyPolicy(KEY));

    ArgumentCaptor<GetKeyPolicyRequest> get = ArgumentCaptor.forClass(GetKeyPolicyRequest.class);
    verify(client).getKeyPolicy(get.capture());
    assertEquals("key-1", get.getValue().getKeyId());
    assertEquals("default", get.getValue().getPolicyName());
  }

  @Test
  void policyCapabilitiesFailClosedOnConditions() {
    var capabilities = kms.getPolicyCapabilities();

    assertTrue(capabilities.isSupportsDeny());
    assertEquals(false, capabilities.isSupportsConditions());
    assertEquals(
        java.util.EnumSet.of(
            PrincipalType.IAM_ROLE, PrincipalType.SERVICE_ROLE, PrincipalType.ACCOUNT),
        capabilities.getSupportedPrincipalTypes());
    assertTrue(capabilities.getSupportedActions().contains(KeyAction.MANAGE));
    assertEquals(false, capabilities.getSupportedActions().contains(KeyAction.SIGN));
  }

  @Test
  void setKeyPolicyRejectsConditions() {
    KeyPolicy policy =
        KeyPolicy.builder()
            .grant(
                KeyGrant.builder()
                    .principal(KeyPrincipal.of(PrincipalType.IAM_ROLE, "r"))
                    .action(KeyAction.DECRYPT)
                    .condition(
                        com.salesforce.multicloudj.kms.model.KeyCondition.builder()
                            .operator(
                                com.salesforce.multicloudj.kms.model.ConditionOperator
                                    .STRING_EQUALS)
                            .key("k")
                            .value("v")
                            .build())
                    .build())
            .build();

    assertThrows(UnSupportedOperationException.class, () -> kms.setKeyPolicy(KEY, policy));
    verifyNoInteractions(client);
  }

  @Test
  void tagKeySendsTagsAsJsonArray() throws Exception {
    kms.tagKey(KEY, Map.of("env", "prod"));

    ArgumentCaptor<TagResourceRequest> captor = ArgumentCaptor.forClass(TagResourceRequest.class);
    verify(client).tagResource(captor.capture());
    assertEquals("key-1", captor.getValue().getKeyId());
    JsonNode tags = MAPPER.readTree(captor.getValue().getTags());
    assertEquals(1, tags.size());
    assertEquals("env", tags.get(0).get("TagKey").asText());
    assertEquals("prod", tags.get(0).get("TagValue").asText());
  }

  @Test
  void encryptBase64EncodesPlaintextAndDecodesCiphertext() throws Exception {
    byte[] plaintext = "secret".getBytes(StandardCharsets.UTF_8);
    byte[] ciphertext = {5, 6, 7, 8};
    when(client.encrypt(any()))
        .thenReturn(
            new EncryptResponse()
                .setBody(
                    new EncryptResponseBody()
                        .setKeyId("key-1")
                        .setKeyVersionId("ver-1")
                        .setCiphertextBlob(Base64.getEncoder().encodeToString(ciphertext))));

    com.salesforce.multicloudj.kms.model.EncryptResponse response =
        kms.encrypt(
            com.salesforce.multicloudj.kms.model.EncryptRequest.builder()
                .keyRef(KEY)
                .plaintext(plaintext)
                .encryptionContextEntry("purpose", "test")
                .build());

    ArgumentCaptor<EncryptRequest> captor = ArgumentCaptor.forClass(EncryptRequest.class);
    verify(client).encrypt(captor.capture());
    assertEquals("key-1", captor.getValue().getKeyId());
    assertEquals(Base64.getEncoder().encodeToString(plaintext), captor.getValue().getPlaintext());
    assertEquals(Map.of("purpose", "test"), captor.getValue().getEncryptionContext());
    assertArrayEquals(ciphertext, response.getCiphertext());
    assertEquals("key-1", response.getKeyRef().getKeyId());
    assertEquals("ver-1", response.getKeyRef().getVersionId());
    assertEquals("ali", response.getKeyRef().getProviderId());
  }

  @Test
  void encryptOmitsEmptyEncryptionContext() throws Exception {
    when(client.encrypt(any()))
        .thenReturn(
            new EncryptResponse()
                .setBody(
                    new EncryptResponseBody()
                        .setKeyId("key-1")
                        .setCiphertextBlob(Base64.getEncoder().encodeToString(new byte[] {1}))));

    kms.encrypt(
        com.salesforce.multicloudj.kms.model.EncryptRequest.builder()
            .keyRef(KEY)
            .plaintext(new byte[] {1})
            .build());

    ArgumentCaptor<EncryptRequest> captor = ArgumentCaptor.forClass(EncryptRequest.class);
    verify(client).encrypt(captor.capture());
    assertNull(captor.getValue().getEncryptionContext());
  }

  @Test
  void decryptReEncodesCiphertextAndDecodesPlaintext() throws Exception {
    byte[] plaintext = {0, 1, 2, (byte) 0xff};
    byte[] ciphertext = {5, 6, 7, 8};
    when(client.decrypt(any()))
        .thenReturn(
            new DecryptResponse()
                .setBody(
                    new DecryptResponseBody()
                        .setKeyId("key-1")
                        .setKeyVersionId("ver-1")
                        .setPlaintext(Base64.getEncoder().encodeToString(plaintext))));

    com.salesforce.multicloudj.kms.model.DecryptResponse response =
        kms.decrypt(
            com.salesforce.multicloudj.kms.model.DecryptRequest.builder()
                .ciphertext(ciphertext)
                .encryptionContextEntry("purpose", "test")
                .build());

    ArgumentCaptor<DecryptRequest> captor = ArgumentCaptor.forClass(DecryptRequest.class);
    verify(client).decrypt(captor.capture());
    assertEquals(
        Base64.getEncoder().encodeToString(ciphertext), captor.getValue().getCiphertextBlob());
    assertEquals(Map.of("purpose", "test"), captor.getValue().getEncryptionContext());
    assertArrayEquals(plaintext, response.getPlaintext());
    assertEquals("key-1", response.getKeyRef().getKeyId());
    assertEquals("ver-1", response.getKeyRef().getVersionId());
  }

  @Test
  void teaExceptionsPropagateFromDriverAndAreMappedByClient() throws Exception {
    TeaException notFound = new TeaException();
    notFound.setCode("Forbidden.KeyNotFound");
    notFound.setStatusCode(404);
    org.mockito.Mockito.doThrow(notFound).when(client).enableKey(any());

    assertSame(notFound, assertThrows(TeaException.class, () -> kms.enableKey(KEY)));

    try (KmsClient publicClient = new KmsClient(kms) {}) {
      assertThrows(ResourceNotFoundException.class, () -> publicClient.enableKey(KEY));
    }
  }

  @Test
  void checkedExceptionsAreWrappedWithOriginalCause() throws Exception {
    TeaException notFound = new TeaException();
    notFound.setCode("EntityNotExist.Key");
    Exception checked = new Exception("checked", notFound);
    org.mockito.Mockito.doThrow(checked).when(client).disableKey(any());

    RuntimeException thrown = assertThrows(RuntimeException.class, () -> kms.disableKey(KEY));

    assertEquals(AliKms.KmsCallException.class, thrown.getClass());
    assertSame(checked, thrown.getCause());
    SubstrateSdkException mapped = kms.mapException(thrown);
    assertEquals(ResourceNotFoundException.class, mapped.getClass());
  }

  @Test
  void publicClientValidationErrorsAreInvalidArgument() throws Exception {
    try (KmsClient publicClient = new KmsClient(kms) {}) {
      assertThrows(
          InvalidArgumentException.class, () -> publicClient.enableKey(KeyRef.builder().build()));
    }
  }

  @Test
  void buildWithoutRegionOrEndpointFails() {
    assertThrows(InvalidArgumentException.class, () -> new AliKms.Builder().build());
  }

  @Test
  void closeDoesNotFail() {
    kms.close();
  }
}
