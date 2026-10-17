package com.salesforce.multicloudj.kms.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.kms.model.ConditionOperator;
import com.salesforce.multicloudj.kms.model.CreateKeyRequest;
import com.salesforce.multicloudj.kms.model.DecryptRequest;
import com.salesforce.multicloudj.kms.model.Effect;
import com.salesforce.multicloudj.kms.model.EncryptRequest;
import com.salesforce.multicloudj.kms.model.ExpirationModel;
import com.salesforce.multicloudj.kms.model.GetImportParametersRequest;
import com.salesforce.multicloudj.kms.model.ImportKeyMaterialRequest;
import com.salesforce.multicloudj.kms.model.KeyAction;
import com.salesforce.multicloudj.kms.model.KeyCondition;
import com.salesforce.multicloudj.kms.model.KeyGrant;
import com.salesforce.multicloudj.kms.model.KeyPolicy;
import com.salesforce.multicloudj.kms.model.KeyPrincipal;
import com.salesforce.multicloudj.kms.model.KeyRef;
import com.salesforce.multicloudj.kms.model.PolicyCapabilities;
import com.salesforce.multicloudj.kms.model.PrincipalType;
import java.time.Instant;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class AbstractKmsTest {

  private static final KeyRef KEY = KeyRef.builder().keyId("key-1").build();
  private static final KeyPrincipal ROLE = KeyPrincipal.of(PrincipalType.IAM_ROLE, "role-1");

  private final TestKms kms = new TestKms();

  @Test
  void keyOperationsRequireKeyId() {
    KeyRef blank = KeyRef.builder().keyId(" ").build();

    assertThrows(InvalidArgumentException.class, () -> kms.enableKey(null));
    assertThrows(InvalidArgumentException.class, () -> kms.disableKey(blank));
    assertThrows(InvalidArgumentException.class, () -> kms.getKeyPolicy(blank));
    assertTrue(kms.calls.isEmpty());
  }

  @Test
  void createKeyRejectsBlankTagNames() {
    CreateKeyRequest request = CreateKeyRequest.builder().tag(" ", "value").build();

    assertThrows(InvalidArgumentException.class, () -> kms.createKey(request));
    assertTrue(kms.calls.isEmpty());
  }

  @Test
  void createKeyAppliesPortableDefaults() {
    kms.createKey(CreateKeyRequest.builder().build());

    assertEquals(List.of("createKey"), kms.calls);
  }

  @Test
  void tagKeyRequiresTags() {
    assertThrows(InvalidArgumentException.class, () -> kms.tagKey(KEY, Map.of()));
    assertThrows(InvalidArgumentException.class, () -> kms.tagKey(KEY, null));
    assertTrue(kms.calls.isEmpty());
  }

  @Test
  void getImportParametersRequiresWrappingAlgorithm() {
    GetImportParametersRequest request =
        GetImportParametersRequest.builder().keyRef(KEY).wrappingAlgorithm(null).build();

    assertThrows(InvalidArgumentException.class, () -> kms.getImportParameters(request));
  }

  @Test
  void importKeyMaterialValidatesTokenMaterialAndExpiry() {
    ImportKeyMaterialRequest.ImportKeyMaterialRequestBuilder valid =
        ImportKeyMaterialRequest.builder()
            .keyRef(KEY)
            .importToken("token")
            .wrappedKeyMaterial(new byte[] {1});

    assertThrows(
        InvalidArgumentException.class,
        () -> kms.importKeyMaterial(valid.importToken(" ").build()));
    assertThrows(
        InvalidArgumentException.class,
        () ->
            kms.importKeyMaterial(
                valid.importToken("token").wrappedKeyMaterial(new byte[0]).build()));
    assertThrows(
        InvalidArgumentException.class,
        () ->
            kms.importKeyMaterial(
                valid
                    .wrappedKeyMaterial(new byte[] {1})
                    .expirationModel(ExpirationModel.KEY_MATERIAL_EXPIRES)
                    .build()));
    assertTrue(kms.calls.isEmpty());

    kms.importKeyMaterial(valid.validTo(Instant.now().plusSeconds(3600)).build());
    assertEquals(List.of("importKeyMaterial"), kms.calls);
  }

  @Test
  void encryptRequiresPlaintextAndDecryptRequiresCiphertext() {
    assertThrows(
        InvalidArgumentException.class,
        () -> kms.encrypt(EncryptRequest.builder().keyRef(KEY).plaintext(new byte[0]).build()));
    assertThrows(
        InvalidArgumentException.class,
        () -> kms.decrypt(DecryptRequest.builder().ciphertext(null).build()));
  }

  @Test
  void encryptIsUnsupportedUnlessProviderImplementsIt() {
    EncryptRequest request = EncryptRequest.builder().keyRef(KEY).plaintext(new byte[] {1}).build();

    assertThrows(UnSupportedOperationException.class, () -> kms.encrypt(request));
  }

  @Test
  void decryptAllowsMissingKeyRef() {
    DecryptRequest request = DecryptRequest.builder().ciphertext(new byte[] {1}).build();

    // Validation passes; the test provider does not implement decrypt.
    assertThrows(UnSupportedOperationException.class, () -> kms.decrypt(request));
  }

  @Test
  void setKeyPolicyRejectsStructurallyInvalidPolicies() {
    KeyPolicy empty = KeyPolicy.builder().build();
    KeyPolicy noPrincipal =
        KeyPolicy.builder().grant(KeyGrant.builder().action(KeyAction.ENCRYPT).build()).build();
    KeyPolicy noAction =
        KeyPolicy.builder().grant(KeyGrant.builder().principal(ROLE).build()).build();
    KeyPolicy blankPrincipal = policyWith(KeyPrincipal.of(PrincipalType.IAM_ROLE, " "));

    assertThrows(InvalidArgumentException.class, () -> kms.setKeyPolicy(KEY, null));
    assertThrows(InvalidArgumentException.class, () -> kms.setKeyPolicy(KEY, empty));
    assertThrows(InvalidArgumentException.class, () -> kms.setKeyPolicy(KEY, noPrincipal));
    assertThrows(InvalidArgumentException.class, () -> kms.setKeyPolicy(KEY, noAction));
    assertThrows(InvalidArgumentException.class, () -> kms.setKeyPolicy(KEY, blankPrincipal));
    assertTrue(kms.calls.isEmpty());
  }

  @Test
  void setKeyPolicyRejectsDenyWhenProviderCannotExpressIt() {
    kms.capabilities = PolicyCapabilities.builder().supportsDeny(false).build();
    KeyPolicy deny =
        KeyPolicy.builder()
            .grant(
                KeyGrant.builder()
                    .principal(ROLE)
                    .action(KeyAction.DECRYPT)
                    .effect(Effect.DENY)
                    .build())
            .build();

    assertThrows(UnSupportedOperationException.class, () -> kms.setKeyPolicy(KEY, deny));
    assertTrue(kms.calls.isEmpty());
  }

  @Test
  void setKeyPolicyRejectsConditionsWhenProviderCannotExpressThem() {
    kms.capabilities = PolicyCapabilities.builder().supportsConditions(false).build();
    KeyPolicy conditional =
        KeyPolicy.builder()
            .grant(
                KeyGrant.builder()
                    .principal(ROLE)
                    .action(KeyAction.DECRYPT)
                    .condition(
                        KeyCondition.builder()
                            .operator(ConditionOperator.STRING_EQUALS)
                            .key("context-key")
                            .value("expected")
                            .build())
                    .build())
            .build();

    assertThrows(UnSupportedOperationException.class, () -> kms.setKeyPolicy(KEY, conditional));
    assertTrue(kms.calls.isEmpty());
  }

  @Test
  void setKeyPolicyRejectsUnsupportedPrincipalTypesAndActions() {
    kms.capabilities =
        PolicyCapabilities.builder()
            .supportedPrincipalTypes(EnumSet.of(PrincipalType.IAM_ROLE))
            .supportedActions(EnumSet.of(KeyAction.ENCRYPT, KeyAction.DECRYPT))
            .build();

    assertThrows(
        UnSupportedOperationException.class,
        () -> kms.setKeyPolicy(KEY, policyWith(KeyPrincipal.of(PrincipalType.WILDCARD, "role-*"))));
    KeyPolicy manage =
        KeyPolicy.builder()
            .grant(KeyGrant.builder().principal(ROLE).action(KeyAction.MANAGE).build())
            .build();
    assertThrows(UnSupportedOperationException.class, () -> kms.setKeyPolicy(KEY, manage));
    assertTrue(kms.calls.isEmpty());

    kms.setKeyPolicy(KEY, policyWith(ROLE));
    assertEquals(List.of("setKeyPolicy"), kms.calls);
  }

  private static KeyPolicy policyWith(KeyPrincipal principal) {
    return KeyPolicy.builder()
        .grant(KeyGrant.builder().principal(principal).action(KeyAction.ENCRYPT).build())
        .build();
  }
}
