package com.salesforce.multicloudj.kms.aws;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.salesforce.multicloudj.common.exceptions.FailedPreconditionException;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceAlreadyExistsException;
import com.salesforce.multicloudj.common.exceptions.ResourceExhaustedException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
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
import com.salesforce.multicloudj.kms.model.PolicyCapabilities;
import com.salesforce.multicloudj.kms.model.PrincipalType;
import com.salesforce.multicloudj.kms.model.ProtectionLevel;
import com.salesforce.multicloudj.kms.model.WrappingAlgorithm;
import com.salesforce.multicloudj.kms.model.WrappingKeySpec;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.AlgorithmSpec;
import software.amazon.awssdk.services.kms.model.CreateAliasRequest;
import software.amazon.awssdk.services.kms.model.DisableKeyRequest;
import software.amazon.awssdk.services.kms.model.EnableKeyRequest;
import software.amazon.awssdk.services.kms.model.ExpirationModelType;
import software.amazon.awssdk.services.kms.model.GetKeyPolicyRequest;
import software.amazon.awssdk.services.kms.model.GetKeyPolicyResponse;
import software.amazon.awssdk.services.kms.model.GetParametersForImportRequest;
import software.amazon.awssdk.services.kms.model.GetParametersForImportResponse;
import software.amazon.awssdk.services.kms.model.KeyMetadata;
import software.amazon.awssdk.services.kms.model.KeySpec;
import software.amazon.awssdk.services.kms.model.KeyUsageType;
import software.amazon.awssdk.services.kms.model.NotFoundException;
import software.amazon.awssdk.services.kms.model.OriginType;
import software.amazon.awssdk.services.kms.model.PutKeyPolicyRequest;
import software.amazon.awssdk.services.kms.model.TagResourceRequest;

@ExtendWith(MockitoExtension.class)
class AwsKmsTest {

  private static final String REGION = "us-west-2";
  private static final String KEY_ID = "1234abcd-12ab-34cd-56ef-1234567890ab";
  private static final String KEY_ARN = "arn:aws:kms:us-west-2:654654370895:key/" + KEY_ID;

  @Mock private KmsClient kmsClient;

  private AwsKms kms;

  @BeforeEach
  void setUp() {
    kms = new AwsKms.Builder().withRegion(REGION).withKmsClient(kmsClient).build();
  }

  private static KeyMetadata.Builder metadata(
      software.amazon.awssdk.services.kms.model.KeyState state) {
    return KeyMetadata.builder()
        .keyId(KEY_ID)
        .arn(KEY_ARN)
        .keyState(state)
        .creationDate(Instant.parse("2025-01-02T03:04:05Z"))
        .description("desc");
  }

  private static software.amazon.awssdk.services.kms.model.CreateKeyResponse created(
      KeyMetadata metadata) {
    return software.amazon.awssdk.services.kms.model.CreateKeyResponse.builder()
        .keyMetadata(metadata)
        .build();
  }

  @Test
  void providerIdIsAws() {
    assertEquals("aws", kms.getProviderId());
    assertEquals("aws", new AwsKms().getProviderId());
    assertInstanceOf(AwsKms.Builder.class, kms.builder());
  }

  @Test
  void policyCapabilitiesAllowEverything() {
    PolicyCapabilities capabilities = kms.getPolicyCapabilities();

    assertTrue(capabilities.isSupportsDeny());
    assertTrue(capabilities.isSupportsConditions());
    assertEquals(EnumSet.allOf(PrincipalType.class), capabilities.getSupportedPrincipalTypes());
    assertEquals(EnumSet.allOf(KeyAction.class), capabilities.getSupportedActions());
  }

  @Test
  void createKeyMapsRequestAliasAndResponse() {
    when(kmsClient.createKey(any(software.amazon.awssdk.services.kms.model.CreateKeyRequest.class)))
        .thenReturn(
            created(metadata(software.amazon.awssdk.services.kms.model.KeyState.ENABLED).build()));

    Map<String, String> tags = new LinkedHashMap<>();
    tags.put("team", "payments");
    CreateKeyResponse response =
        kms.createKey(
            CreateKeyRequest.builder()
                .keyName("orders")
                .description("order key")
                .tags(tags)
                .protectionLevel(ProtectionLevel.HSM)
                .multiRegion(true)
                .build());

    ArgumentCaptor<software.amazon.awssdk.services.kms.model.CreateKeyRequest> create =
        ArgumentCaptor.forClass(software.amazon.awssdk.services.kms.model.CreateKeyRequest.class);
    verify(kmsClient).createKey(create.capture());
    assertEquals(KeySpec.SYMMETRIC_DEFAULT, create.getValue().keySpec());
    assertEquals(KeyUsageType.ENCRYPT_DECRYPT, create.getValue().keyUsage());
    assertEquals(OriginType.AWS_KMS, create.getValue().origin());
    assertEquals("order key", create.getValue().description());
    assertTrue(create.getValue().multiRegion());
    assertEquals(1, create.getValue().tags().size());
    assertEquals("team", create.getValue().tags().get(0).tagKey());
    assertEquals("payments", create.getValue().tags().get(0).tagValue());

    ArgumentCaptor<CreateAliasRequest> alias = ArgumentCaptor.forClass(CreateAliasRequest.class);
    verify(kmsClient).createAlias(alias.capture());
    assertEquals("alias/orders", alias.getValue().aliasName());
    assertEquals(KEY_ID, alias.getValue().targetKeyId());

    assertEquals("aws", response.getKeyRef().getProviderId());
    assertEquals(REGION, response.getKeyRef().getRegion());
    assertEquals(KEY_ID, response.getKeyRef().getKeyId());
    assertEquals(KEY_ARN, response.getKeyRef().getResourceName());
    assertNull(response.getKeyRef().getVersionId());
    assertEquals(KeyState.ENABLED, response.getKeyState());
    assertEquals(Instant.parse("2025-01-02T03:04:05Z"), response.getCreationTime());
    assertEquals("desc", response.getDescription());
  }

  @Test
  void createExternalKeyWithoutNameCreatesNoAlias() {
    when(kmsClient.createKey(any(software.amazon.awssdk.services.kms.model.CreateKeyRequest.class)))
        .thenReturn(
            created(
                metadata(software.amazon.awssdk.services.kms.model.KeyState.PENDING_IMPORT)
                    .build()));

    CreateKeyResponse response =
        kms.createKey(CreateKeyRequest.builder().origin(KeyOrigin.EXTERNAL).build());

    ArgumentCaptor<software.amazon.awssdk.services.kms.model.CreateKeyRequest> create =
        ArgumentCaptor.forClass(software.amazon.awssdk.services.kms.model.CreateKeyRequest.class);
    verify(kmsClient).createKey(create.capture());
    assertEquals(OriginType.EXTERNAL, create.getValue().origin());
    assertFalse(create.getValue().multiRegion());
    assertFalse(create.getValue().hasTags());
    verify(kmsClient, never()).createAlias(any(CreateAliasRequest.class));
    assertEquals(KeyState.PENDING_IMPORT, response.getKeyState());
  }

  @Test
  void createKeyMapsEveryNativeKeyState() {
    Map<software.amazon.awssdk.services.kms.model.KeyState, KeyState> expected =
        new LinkedHashMap<>();
    expected.put(software.amazon.awssdk.services.kms.model.KeyState.CREATING, KeyState.CREATING);
    expected.put(software.amazon.awssdk.services.kms.model.KeyState.ENABLED, KeyState.ENABLED);
    expected.put(software.amazon.awssdk.services.kms.model.KeyState.DISABLED, KeyState.DISABLED);
    expected.put(
        software.amazon.awssdk.services.kms.model.KeyState.PENDING_IMPORT, KeyState.PENDING_IMPORT);
    expected.put(
        software.amazon.awssdk.services.kms.model.KeyState.PENDING_DELETION,
        KeyState.PENDING_DELETION);
    expected.put(
        software.amazon.awssdk.services.kms.model.KeyState.PENDING_REPLICA_DELETION,
        KeyState.PENDING_DELETION);
    expected.put(
        software.amazon.awssdk.services.kms.model.KeyState.UNAVAILABLE, KeyState.UNAVAILABLE);
    expected.put(software.amazon.awssdk.services.kms.model.KeyState.UPDATING, KeyState.UNKNOWN);

    for (Map.Entry<software.amazon.awssdk.services.kms.model.KeyState, KeyState> entry :
        expected.entrySet()) {
      when(kmsClient.createKey(
              any(software.amazon.awssdk.services.kms.model.CreateKeyRequest.class)))
          .thenReturn(created(metadata(entry.getKey()).build()));

      assertEquals(
          entry.getValue(),
          kms.createKey(CreateKeyRequest.builder().build()).getKeyState(),
          "native state " + entry.getKey());
    }
  }

  @Test
  void enableAndDisablePreferResourceNameThenKeyId() {
    kms.enableKey(KeyRef.builder().keyId(KEY_ID).resourceName(KEY_ARN).build());
    kms.disableKey(KeyRef.builder().keyId(KEY_ID).build());

    ArgumentCaptor<EnableKeyRequest> enable = ArgumentCaptor.forClass(EnableKeyRequest.class);
    verify(kmsClient).enableKey(enable.capture());
    assertEquals(KEY_ARN, enable.getValue().keyId());
    ArgumentCaptor<DisableKeyRequest> disable = ArgumentCaptor.forClass(DisableKeyRequest.class);
    verify(kmsClient).disableKey(disable.capture());
    assertEquals(KEY_ID, disable.getValue().keyId());
  }

  @Test
  void enableAndDisableRejectVersionedReferences() {
    KeyRef versioned = KeyRef.builder().keyId(KEY_ID).versionId("1").build();

    assertThrows(UnSupportedOperationException.class, () -> kms.enableKey(versioned));
    assertThrows(UnSupportedOperationException.class, () -> kms.disableKey(versioned));
    verifyNoInteractions(kmsClient);
  }

  @Test
  void getImportParametersDefaultsToRsa4096() {
    Instant expiry = Instant.parse("2025-06-01T00:00:00Z");
    byte[] token = {1, 2, 3, 4};
    byte[] publicKey = {9, 8, 7};
    when(kmsClient.getParametersForImport(any(GetParametersForImportRequest.class)))
        .thenReturn(
            GetParametersForImportResponse.builder()
                .keyId(KEY_ARN)
                .importToken(SdkBytes.fromByteArray(token))
                .publicKey(SdkBytes.fromByteArray(publicKey))
                .parametersValidTo(expiry)
                .build());
    KeyRef keyRef = KeyRef.builder().keyId(KEY_ID).build();

    ImportParameters parameters =
        kms.getImportParameters(GetImportParametersRequest.builder().keyRef(keyRef).build());

    ArgumentCaptor<GetParametersForImportRequest> captor =
        ArgumentCaptor.forClass(GetParametersForImportRequest.class);
    verify(kmsClient).getParametersForImport(captor.capture());
    assertEquals(KEY_ID, captor.getValue().keyId());
    assertEquals(AlgorithmSpec.RSAES_OAEP_SHA_256, captor.getValue().wrappingAlgorithm());
    assertEquals(
        software.amazon.awssdk.services.kms.model.WrappingKeySpec.RSA_4096,
        captor.getValue().wrappingKeySpec());

    assertSame(keyRef, parameters.getKeyRef());
    assertEquals(Base64.getEncoder().encodeToString(token), parameters.getImportToken());
    assertArrayEquals(publicKey, parameters.getWrappingPublicKey());
    assertEquals(WrappingAlgorithm.RSAES_OAEP_SHA_256, parameters.getWrappingAlgorithm());
    assertEquals(WrappingKeySpec.RSA_4096, parameters.getWrappingKeySpec());
    assertEquals(expiry, parameters.getTokenExpiry());
  }

  @Test
  void getImportParametersMapsExplicitAlgorithmAndKeySpecs() {
    when(kmsClient.getParametersForImport(any(GetParametersForImportRequest.class)))
        .thenReturn(
            GetParametersForImportResponse.builder()
                .importToken(SdkBytes.fromByteArray(new byte[] {1}))
                .publicKey(SdkBytes.fromByteArray(new byte[] {2}))
                .build());
    KeyRef keyRef = KeyRef.builder().keyId(KEY_ID).build();

    ImportParameters wrap =
        kms.getImportParameters(
            GetImportParametersRequest.builder()
                .keyRef(keyRef)
                .wrappingAlgorithm(WrappingAlgorithm.RSA_AES_KEY_WRAP_SHA_256)
                .wrappingKeySpec(WrappingKeySpec.RSA_2048)
                .build());
    kms.getImportParameters(
        GetImportParametersRequest.builder()
            .keyRef(keyRef)
            .wrappingKeySpec(WrappingKeySpec.RSA_3072)
            .build());

    ArgumentCaptor<GetParametersForImportRequest> captor =
        ArgumentCaptor.forClass(GetParametersForImportRequest.class);
    verify(kmsClient, org.mockito.Mockito.times(2)).getParametersForImport(captor.capture());
    assertEquals(
        AlgorithmSpec.RSA_AES_KEY_WRAP_SHA_256, captor.getAllValues().get(0).wrappingAlgorithm());
    assertEquals(
        software.amazon.awssdk.services.kms.model.WrappingKeySpec.RSA_2048,
        captor.getAllValues().get(0).wrappingKeySpec());
    assertEquals(
        software.amazon.awssdk.services.kms.model.WrappingKeySpec.RSA_3072,
        captor.getAllValues().get(1).wrappingKeySpec());
    assertEquals(WrappingAlgorithm.RSA_AES_KEY_WRAP_SHA_256, wrap.getWrappingAlgorithm());
    assertEquals(WrappingKeySpec.RSA_2048, wrap.getWrappingKeySpec());
    assertNull(wrap.getTokenExpiry());
  }

  @Test
  void importKeyMaterialDecodesTokenAndReturnsRequestKeyRef() {
    byte[] token = {5, 6, 7};
    byte[] material = {10, 11, 12};
    KeyRef keyRef = KeyRef.builder().keyId(KEY_ID).build();

    KeyRef result =
        kms.importKeyMaterial(
            ImportKeyMaterialRequest.builder()
                .keyRef(keyRef)
                .importToken(Base64.getEncoder().encodeToString(token))
                .wrappedKeyMaterial(material)
                .build());

    ArgumentCaptor<software.amazon.awssdk.services.kms.model.ImportKeyMaterialRequest> captor =
        ArgumentCaptor.forClass(
            software.amazon.awssdk.services.kms.model.ImportKeyMaterialRequest.class);
    verify(kmsClient).importKeyMaterial(captor.capture());
    assertEquals(KEY_ID, captor.getValue().keyId());
    assertArrayEquals(token, captor.getValue().importToken().asByteArray());
    assertArrayEquals(material, captor.getValue().encryptedKeyMaterial().asByteArray());
    assertEquals(
        ExpirationModelType.KEY_MATERIAL_DOES_NOT_EXPIRE, captor.getValue().expirationModel());
    assertNull(captor.getValue().validTo());
    assertSame(keyRef, result);
  }

  @Test
  void importKeyMaterialMapsExpiry() {
    Instant validTo = Instant.parse("2030-01-01T00:00:00Z");

    kms.importKeyMaterial(
        ImportKeyMaterialRequest.builder()
            .keyRef(KeyRef.builder().keyId(KEY_ID).build())
            .importToken(Base64.getEncoder().encodeToString(new byte[] {1}))
            .wrappedKeyMaterial(new byte[] {2})
            .expirationModel(ExpirationModel.KEY_MATERIAL_EXPIRES)
            .validTo(validTo)
            .build());

    ArgumentCaptor<software.amazon.awssdk.services.kms.model.ImportKeyMaterialRequest> captor =
        ArgumentCaptor.forClass(
            software.amazon.awssdk.services.kms.model.ImportKeyMaterialRequest.class);
    verify(kmsClient).importKeyMaterial(captor.capture());
    assertEquals(ExpirationModelType.KEY_MATERIAL_EXPIRES, captor.getValue().expirationModel());
    assertEquals(validTo, captor.getValue().validTo());
  }

  @Test
  void importKeyMaterialWithMalformedTokenFailsBeforeCallingService() {
    ImportKeyMaterialRequest request =
        ImportKeyMaterialRequest.builder()
            .keyRef(KeyRef.builder().keyId(KEY_ID).build())
            .importToken("***not base64***")
            .wrappedKeyMaterial(new byte[] {2})
            .build();

    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, () -> kms.importKeyMaterial(request));

    assertInstanceOf(InvalidArgumentException.class, kms.mapException(thrown));
    verifyNoInteractions(kmsClient);
  }

  @Test
  void setKeyPolicyWritesDefaultPolicy() {
    KeyPolicy policy =
        KeyPolicy.builder()
            .grant(
                KeyGrant.builder()
                    .principal(KeyPrincipal.of(PrincipalType.ACCOUNT, "654654370895"))
                    .action(KeyAction.MANAGE)
                    .build())
            .build();

    kms.setKeyPolicy(KeyRef.builder().keyId(KEY_ID).build(), policy);

    ArgumentCaptor<PutKeyPolicyRequest> captor = ArgumentCaptor.forClass(PutKeyPolicyRequest.class);
    verify(kmsClient).putKeyPolicy(captor.capture());
    assertEquals(KEY_ID, captor.getValue().keyId());
    assertEquals("default", captor.getValue().policyName());
    assertEquals(policy, AwsKeyPolicyTranslator.fromAwsPolicy(captor.getValue().policy()));
  }

  @Test
  void getKeyPolicyReadsDefaultPolicy() {
    KeyPolicy policy =
        KeyPolicy.builder()
            .grant(
                KeyGrant.builder()
                    .principal(KeyPrincipal.of(PrincipalType.IAM_ROLE, "arn:aws:iam::1:role/r"))
                    .action(KeyAction.ENCRYPT)
                    .action(KeyAction.DECRYPT)
                    .build())
            .build();
    when(kmsClient.getKeyPolicy(any(GetKeyPolicyRequest.class)))
        .thenReturn(
            GetKeyPolicyResponse.builder()
                .policy(AwsKeyPolicyTranslator.toAwsPolicy(policy))
                .build());

    KeyPolicy read = kms.getKeyPolicy(KeyRef.builder().keyId(KEY_ID).build());

    ArgumentCaptor<GetKeyPolicyRequest> captor = ArgumentCaptor.forClass(GetKeyPolicyRequest.class);
    verify(kmsClient).getKeyPolicy(captor.capture());
    assertEquals(KEY_ID, captor.getValue().keyId());
    assertEquals("default", captor.getValue().policyName());
    assertEquals(policy, read);
  }

  @Test
  void tagKeyMapsTags() {
    kms.tagKey(KeyRef.builder().keyId(KEY_ID).build(), Map.of("env", "prod"));

    ArgumentCaptor<TagResourceRequest> captor = ArgumentCaptor.forClass(TagResourceRequest.class);
    verify(kmsClient).tagResource(captor.capture());
    assertEquals(KEY_ID, captor.getValue().keyId());
    assertEquals(1, captor.getValue().tags().size());
    assertEquals("env", captor.getValue().tags().get(0).tagKey());
    assertEquals("prod", captor.getValue().tags().get(0).tagValue());
  }

  @Test
  void encryptMapsRequestAndResponse() {
    byte[] plaintext = "secret".getBytes(StandardCharsets.UTF_8);
    byte[] ciphertext = {4, 3, 2, 1};
    when(kmsClient.encrypt(any(software.amazon.awssdk.services.kms.model.EncryptRequest.class)))
        .thenReturn(
            software.amazon.awssdk.services.kms.model.EncryptResponse.builder()
                .ciphertextBlob(SdkBytes.fromByteArray(ciphertext))
                .keyId(KEY_ARN)
                .build());

    EncryptResponse response =
        kms.encrypt(
            EncryptRequest.builder()
                .keyRef(KeyRef.builder().keyId(KEY_ID).build())
                .plaintext(plaintext)
                .encryptionContextEntry("purpose", "test")
                .build());

    ArgumentCaptor<software.amazon.awssdk.services.kms.model.EncryptRequest> captor =
        ArgumentCaptor.forClass(software.amazon.awssdk.services.kms.model.EncryptRequest.class);
    verify(kmsClient).encrypt(captor.capture());
    assertEquals(KEY_ID, captor.getValue().keyId());
    assertArrayEquals(plaintext, captor.getValue().plaintext().asByteArray());
    assertEquals(Map.of("purpose", "test"), captor.getValue().encryptionContext());

    assertArrayEquals(ciphertext, response.getCiphertext());
    assertEquals(KEY_ID, response.getKeyRef().getKeyId());
    assertEquals(KEY_ARN, response.getKeyRef().getResourceName());
    assertEquals("aws", response.getKeyRef().getProviderId());
    assertEquals(REGION, response.getKeyRef().getRegion());
  }

  @Test
  void encryptWithoutContextSendsNone() {
    when(kmsClient.encrypt(any(software.amazon.awssdk.services.kms.model.EncryptRequest.class)))
        .thenReturn(
            software.amazon.awssdk.services.kms.model.EncryptResponse.builder()
                .ciphertextBlob(SdkBytes.fromByteArray(new byte[] {1}))
                .keyId(KEY_ARN)
                .build());

    kms.encrypt(
        EncryptRequest.builder()
            .keyRef(KeyRef.builder().keyId(KEY_ID).build())
            .plaintext(new byte[] {1})
            .build());

    ArgumentCaptor<software.amazon.awssdk.services.kms.model.EncryptRequest> captor =
        ArgumentCaptor.forClass(software.amazon.awssdk.services.kms.model.EncryptRequest.class);
    verify(kmsClient).encrypt(captor.capture());
    assertFalse(captor.getValue().hasEncryptionContext());
  }

  @Test
  void decryptMapsRequestAndResponseWithKey() {
    byte[] ciphertext = {4, 3, 2, 1};
    byte[] plaintext = {7, 7};
    when(kmsClient.decrypt(any(software.amazon.awssdk.services.kms.model.DecryptRequest.class)))
        .thenReturn(
            software.amazon.awssdk.services.kms.model.DecryptResponse.builder()
                .plaintext(SdkBytes.fromByteArray(plaintext))
                .keyId(KEY_ARN)
                .build());

    DecryptResponse response =
        kms.decrypt(
            DecryptRequest.builder()
                .keyRef(KeyRef.builder().keyId(KEY_ID).build())
                .ciphertext(ciphertext)
                .encryptionContextEntry("purpose", "test")
                .build());

    ArgumentCaptor<software.amazon.awssdk.services.kms.model.DecryptRequest> captor =
        ArgumentCaptor.forClass(software.amazon.awssdk.services.kms.model.DecryptRequest.class);
    verify(kmsClient).decrypt(captor.capture());
    assertEquals(KEY_ID, captor.getValue().keyId());
    assertArrayEquals(ciphertext, captor.getValue().ciphertextBlob().asByteArray());
    assertEquals(Map.of("purpose", "test"), captor.getValue().encryptionContext());
    assertArrayEquals(plaintext, response.getPlaintext());
    assertEquals(KEY_ID, response.getKeyRef().getKeyId());
    assertEquals(KEY_ARN, response.getKeyRef().getResourceName());
  }

  @Test
  void decryptWithoutKeyOmitsKeyId() {
    when(kmsClient.decrypt(any(software.amazon.awssdk.services.kms.model.DecryptRequest.class)))
        .thenReturn(
            software.amazon.awssdk.services.kms.model.DecryptResponse.builder()
                .plaintext(SdkBytes.fromByteArray(new byte[] {1}))
                .keyId(KEY_ARN)
                .build());

    kms.decrypt(DecryptRequest.builder().ciphertext(new byte[] {1, 2}).build());

    ArgumentCaptor<software.amazon.awssdk.services.kms.model.DecryptRequest> captor =
        ArgumentCaptor.forClass(software.amazon.awssdk.services.kms.model.DecryptRequest.class);
    verify(kmsClient).decrypt(captor.capture());
    assertNull(captor.getValue().keyId());
    assertFalse(captor.getValue().hasEncryptionContext());
  }

  @Test
  void nativeExceptionsPropagateFromOperations() {
    NotFoundException notFound = NotFoundException.builder().message("missing").build();
    when(kmsClient.encrypt(any(software.amazon.awssdk.services.kms.model.EncryptRequest.class)))
        .thenThrow(notFound);

    NotFoundException thrown =
        assertThrows(
            NotFoundException.class,
            () ->
                kms.encrypt(
                    EncryptRequest.builder()
                        .keyRef(KeyRef.builder().keyId(KEY_ID).build())
                        .plaintext(new byte[] {1})
                        .build()));

    assertSame(notFound, thrown);
  }

  @Test
  void mapExceptionClassifiesKmsErrorCodes() {
    Map<String, Class<? extends SubstrateSdkException>> expected = new LinkedHashMap<>();
    expected.put("NotFoundException", ResourceNotFoundException.class);
    expected.put("KMSInvalidStateException", FailedPreconditionException.class);
    expected.put("DisabledException", FailedPreconditionException.class);
    expected.put("ExpiredImportTokenException", FailedPreconditionException.class);
    expected.put("InvalidCiphertextException", InvalidArgumentException.class);
    expected.put("InvalidImportTokenException", InvalidArgumentException.class);
    expected.put("IncorrectKeyMaterialException", InvalidArgumentException.class);
    expected.put("MalformedPolicyDocumentException", InvalidArgumentException.class);
    expected.put("TagException", InvalidArgumentException.class);
    expected.put("InvalidKeyUsageException", InvalidArgumentException.class);
    expected.put("AlreadyExistsException", ResourceAlreadyExistsException.class);
    expected.put("LimitExceededException", ResourceExhaustedException.class);
    expected.put("ThrottlingException", ResourceExhaustedException.class);
    expected.put("AccessDeniedException", UnAuthorizedException.class);
    expected.put("KMSInternalException", UnknownException.class);

    expected.forEach(
        (code, type) -> {
          AwsServiceException exception =
              AwsServiceException.builder()
                  .awsErrorDetails(AwsErrorDetails.builder().errorCode(code).build())
                  .build();
          SubstrateSdkException mapped = kms.mapException(exception);
          assertEquals(type, mapped.getClass(), "error code " + code);
          assertSame(exception, mapped.getCause());
        });
  }

  @Test
  void mapExceptionHandlesNonServiceFailures() {
    assertInstanceOf(
        UnknownException.class,
        kms.mapException(AwsServiceException.builder().message("no details").build()));
    assertInstanceOf(
        InvalidArgumentException.class, kms.mapException(SdkClientException.create("bad config")));
    assertInstanceOf(UnknownException.class, kms.mapException(new RuntimeException("boom")));
  }

  @Test
  void mapExceptionPassesSubstrateExceptionsThrough() {
    UnSupportedOperationException original = new UnSupportedOperationException("nope");

    assertSame(original, kms.mapException(original));
  }

  @Test
  void closeClosesNativeClient() {
    kms.close();

    verify(kmsClient).close();
  }
}
