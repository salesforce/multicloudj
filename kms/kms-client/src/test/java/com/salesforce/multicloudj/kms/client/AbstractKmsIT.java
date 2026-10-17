package com.salesforce.multicloudj.kms.client;

import com.salesforce.multicloudj.common.exceptions.FailedPreconditionException;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.common.util.common.TestsUtil;
import com.salesforce.multicloudj.kms.driver.AbstractKms;
import com.salesforce.multicloudj.kms.model.CreateKeyRequest;
import com.salesforce.multicloudj.kms.model.CreateKeyResponse;
import com.salesforce.multicloudj.kms.model.DecryptRequest;
import com.salesforce.multicloudj.kms.model.Effect;
import com.salesforce.multicloudj.kms.model.EncryptRequest;
import com.salesforce.multicloudj.kms.model.EncryptResponse;
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
import com.salesforce.multicloudj.kms.model.WrappingAlgorithm;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.spec.MGF1ParameterSpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import javax.crypto.Cipher;
import javax.crypto.spec.OAEPParameterSpec;
import javax.crypto.spec.PSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractKmsIT {

  private static final byte[] PLAINTEXT =
      "multicloudj-kms-conformance".getBytes(StandardCharsets.UTF_8);
  private static final Map<String, String> ENCRYPTION_CONTEXT = Map.of("purpose", "conformance");

  // Fixed 256-bit key material and seed so the wrapped material, and therefore the recorded
  // import request body, is identical on every replay.
  private static final byte[] KEY_MATERIAL = new byte[32];
  private static final byte[] WRAP_SEED =
      "multicloudj-kms-wrap-seed".getBytes(StandardCharsets.UTF_8);

  static {
    for (int i = 0; i < KEY_MATERIAL.length; i++) {
      KEY_MATERIAL[i] = (byte) i;
    }
  }

  public interface Harness extends AutoCloseable {
    /** Builds the provider with its native client routed through the WireMock proxy. */
    AbstractKms createKms();

    String getProviderId();

    int getPort();

    List<String> getWiremockExtensions();

    String getKmsEndpoint();

    /**
     * Key name for a test. Must be unique per recording on providers whose key names can never be
     * reused, so harnesses may append a recording-specific suffix.
     */
    String getKeyName(String testName);

    /** A key id that is well formed for the provider but does not exist. */
    String getNonexistentKeyId();

    /** Identity that keeps administrative access to test keys when their policy is replaced. */
    KeyPrincipal getAdminPrincipal();

    /** Identity granted encrypt and decrypt access by the policy test. */
    KeyPrincipal getGranteePrincipal();

    /**
     * Releases a key created by a test using the provider's native API. Called while WireMock is
     * still recording so the cleanup calls replay as well.
     */
    void deleteKey(KeyRef keyRef);
  }

  protected abstract Harness createHarness();

  private Harness harness;
  private KmsClient kmsClient;
  private final List<KeyRef> createdKeys = new ArrayList<>();

  @BeforeAll
  public void initializeWireMockServer() {
    harness = createHarness();
    TestsUtil.startWireMockServer(
        "src/test/resources",
        harness.getPort(),
        harness.getWiremockExtensions().toArray(new String[0]));
  }

  @AfterAll
  public void shutdownWireMockServer() throws Exception {
    TestsUtil.stopWireMockServer();
    harness.close();
  }

  @BeforeEach
  public void setupTestEnvironment(TestInfo testInfo) {
    String testClassName = testInfo.getTestClass().map(Class::getSimpleName).orElse("Unknown");
    String testMethodName =
        testInfo.getTestMethod().map(java.lang.reflect.Method::getName).orElse("unknown");
    TestsUtil.startWireMockRecording(harness.getKmsEndpoint(), testClassName, testMethodName);
    kmsClient = new KmsClient(harness.createKms());
  }

  @AfterEach
  public void cleanupTestEnvironment() throws Exception {
    try {
      for (KeyRef keyRef : createdKeys) {
        harness.deleteKey(keyRef);
      }
      createdKeys.clear();
    } finally {
      TestsUtil.stopWireMockRecording();
      if (kmsClient != null) {
        kmsClient.close();
      }
    }
  }

  @Test
  public void testCreateKey() {
    CreateKeyResponse response = createKey("create", KeyOrigin.KMS);

    KeyRef keyRef = response.getKeyRef();
    Assertions.assertNotNull(keyRef);
    Assertions.assertFalse(keyRef.getKeyId().isBlank());
    Assertions.assertEquals(harness.getProviderId(), keyRef.getProviderId());
    Assertions.assertEquals(KeyState.ENABLED, response.getKeyState());
  }

  @Test
  public void testCreateExternalKeyIsPendingImport() {
    CreateKeyResponse response = createKey("external", KeyOrigin.EXTERNAL);

    Assertions.assertEquals(KeyState.PENDING_IMPORT, response.getKeyState());
  }

  @Test
  public void testEncryptDecrypt() {
    KeyRef keyRef = createKey("crypt", KeyOrigin.KMS).getKeyRef();

    EncryptResponse encrypted = encrypt(keyRef);
    Assertions.assertFalse(Arrays.equals(PLAINTEXT, encrypted.getCiphertext()));

    byte[] plaintext =
        kmsClient
            .decrypt(
                DecryptRequest.builder()
                    .keyRef(keyRef)
                    .ciphertext(encrypted.getCiphertext())
                    .encryptionContext(ENCRYPTION_CONTEXT)
                    .build())
            .getPlaintext();
    Assertions.assertArrayEquals(PLAINTEXT, plaintext);
  }

  @Test
  public void testDecryptWithWrongEncryptionContext() {
    KeyRef keyRef = createKey("crypt-ctx", KeyOrigin.KMS).getKeyRef();
    EncryptResponse encrypted = encrypt(keyRef);

    DecryptRequest request =
        DecryptRequest.builder()
            .keyRef(keyRef)
            .ciphertext(encrypted.getCiphertext())
            .encryptionContextEntry("purpose", "something-else")
            .build();
    Assertions.assertThrows(InvalidArgumentException.class, () -> kmsClient.decrypt(request));
  }

  @Test
  public void testDisableAndEnableKey() {
    KeyRef keyRef = createKey("toggle", KeyOrigin.KMS).getKeyRef();

    kmsClient.disableKey(keyRef);
    // Key state changes are eventually consistent, so poll until the key rejects encryption.
    eventually(
        () -> {
          try {
            encrypt(keyRef);
            return false;
          } catch (FailedPreconditionException expected) {
            return true;
          }
        });

    kmsClient.enableKey(keyRef);
    eventually(
        () -> {
          try {
            encrypt(keyRef);
            return true;
          } catch (FailedPreconditionException notYet) {
            return false;
          }
        });
  }

  @Test
  public void testEnableNonexistentKey() {
    KeyRef keyRef = KeyRef.builder().keyId(harness.getNonexistentKeyId()).build();

    Assertions.assertThrows(ResourceNotFoundException.class, () -> kmsClient.enableKey(keyRef));
  }

  @Test
  public void testTagKey() {
    KeyRef keyRef = createKey("tag", KeyOrigin.KMS).getKeyRef();

    Assertions.assertDoesNotThrow(() -> kmsClient.tagKey(keyRef, Map.of("env", "conformance")));
  }

  @Test
  public void testTagNonexistentKey() {
    KeyRef keyRef = KeyRef.builder().keyId(harness.getNonexistentKeyId()).build();

    Assertions.assertThrows(
        ResourceNotFoundException.class, () -> kmsClient.tagKey(keyRef, Map.of("env", "x")));
  }

  @Test
  public void testSetAndGetKeyPolicy() {
    KeyRef keyRef = createKey("policy", KeyOrigin.KMS).getKeyRef();
    KeyPolicy policy =
        KeyPolicy.builder()
            .grant(
                KeyGrant.builder()
                    .principal(harness.getAdminPrincipal())
                    .action(KeyAction.MANAGE)
                    .action(KeyAction.DESCRIBE)
                    .build())
            .grant(
                KeyGrant.builder()
                    .principal(harness.getGranteePrincipal())
                    .action(KeyAction.ENCRYPT)
                    .action(KeyAction.DECRYPT)
                    .build())
            .build();

    kmsClient.setKeyPolicy(keyRef, policy);
    KeyPolicy read = kmsClient.getKeyPolicy(keyRef);

    boolean granteeHasCryptoAccess =
        read.getGrants().stream()
                .filter(grant -> grant.getEffect() == Effect.ALLOW)
                .filter(grant -> grant.getPrincipals().contains(harness.getGranteePrincipal()))
                .flatMap(grant -> grant.getActions().stream())
                .filter(action -> action == KeyAction.ENCRYPT || action == KeyAction.DECRYPT)
                .distinct()
                .count()
            == 2;
    Assertions.assertTrue(granteeHasCryptoAccess, "grantee grant missing from " + read);
  }

  @Test
  public void testSetKeyPolicyRejectsDenyWhenUnsupported() {
    Assumptions.assumeFalse(kmsClient.getPolicyCapabilities().isSupportsDeny());
    KeyRef keyRef = KeyRef.builder().keyId(harness.getNonexistentKeyId()).build();
    KeyPolicy policy =
        KeyPolicy.builder()
            .grant(
                KeyGrant.builder()
                    .principal(harness.getGranteePrincipal())
                    .action(KeyAction.DECRYPT)
                    .effect(Effect.DENY)
                    .build())
            .build();

    Assertions.assertThrows(
        UnSupportedOperationException.class, () -> kmsClient.setKeyPolicy(keyRef, policy));
  }

  @Test
  public void testImportKeyMaterial() throws Exception {
    KeyRef keyRef = createKey("byok", KeyOrigin.EXTERNAL).getKeyRef();

    ImportParameters parameters =
        kmsClient.getImportParameters(
            GetImportParametersRequest.builder()
                .keyRef(keyRef)
                .wrappingAlgorithm(WrappingAlgorithm.RSAES_OAEP_SHA_256)
                .build());
    Assertions.assertEquals(
        WrappingAlgorithm.RSAES_OAEP_SHA_256, parameters.getWrappingAlgorithm());
    Assertions.assertNotNull(parameters.getWrappingKeySpec());
    Assertions.assertFalse(parameters.getImportToken().isBlank());

    KeyRef imported =
        kmsClient.importKeyMaterial(
            ImportKeyMaterialRequest.builder()
                .keyRef(parameters.getKeyRef())
                .importToken(parameters.getImportToken())
                .wrappedKeyMaterial(wrapWithRsaOaepSha256(parameters.getWrappingPublicKey()))
                .build());
    Assertions.assertNotNull(imported);

    // Imported material can take a moment to become usable.
    eventually(
        () -> {
          try {
            encrypt(imported);
            return true;
          } catch (FailedPreconditionException notYet) {
            return false;
          }
        });
  }

  @Test
  public void testGetImportParametersForNonexistentKey() {
    KeyRef keyRef = KeyRef.builder().keyId(harness.getNonexistentKeyId()).build();
    GetImportParametersRequest request =
        GetImportParametersRequest.builder().keyRef(keyRef).build();

    Assertions.assertThrows(
        ResourceNotFoundException.class, () -> kmsClient.getImportParameters(request));
  }

  private CreateKeyResponse createKey(String testName, KeyOrigin origin) {
    CreateKeyResponse response =
        kmsClient.createKey(
            CreateKeyRequest.builder()
                .keyName(harness.getKeyName(testName))
                .origin(origin)
                .description("multicloudj kms conformance test key")
                .tag("suite", "multicloudj-conformance")
                .build());
    createdKeys.add(response.getKeyRef());
    return response;
  }

  private EncryptResponse encrypt(KeyRef keyRef) {
    return kmsClient.encrypt(
        EncryptRequest.builder()
            .keyRef(keyRef)
            .plaintext(PLAINTEXT)
            .encryptionContext(ENCRYPTION_CONTEXT)
            .build());
  }

  private static byte[] wrapWithRsaOaepSha256(byte[] derPublicKey) throws Exception {
    PublicKey publicKey =
        KeyFactory.getInstance("RSA").generatePublic(new X509EncodedKeySpec(derPublicKey));
    SecureRandom deterministic = SecureRandom.getInstance("SHA1PRNG");
    deterministic.setSeed(WRAP_SEED);
    Cipher cipher = Cipher.getInstance("RSA/ECB/OAEPPadding");
    cipher.init(
        Cipher.ENCRYPT_MODE,
        publicKey,
        new OAEPParameterSpec(
            "SHA-256", "MGF1", MGF1ParameterSpec.SHA256, PSource.PSpecified.DEFAULT),
        deterministic);
    return cipher.doFinal(KEY_MATERIAL);
  }

  /**
   * Retries {@code condition} until it holds. Waits between attempts only while recording; replay
   * serves the recorded attempts back in order.
   */
  private static void eventually(Supplier<Boolean> condition) {
    boolean recording = System.getProperty("record") != null;
    for (int attempt = 0; attempt < 30; attempt++) {
      if (condition.get()) {
        return;
      }
      if (recording) {
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(e);
        }
      }
    }
    Assertions.fail("condition did not hold after retries");
  }
}
