package com.salesforce.multicloudj.kms.gcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.iam.v1.Binding;
import com.google.iam.v1.Policy;
import com.google.type.Expr;
import com.salesforce.multicloudj.kms.model.KeyAction;
import com.salesforce.multicloudj.kms.model.KeyGrant;
import com.salesforce.multicloudj.kms.model.KeyPolicy;
import com.salesforce.multicloudj.kms.model.KeyPrincipal;
import com.salesforce.multicloudj.kms.model.PolicyCapabilities;
import com.salesforce.multicloudj.kms.model.PrincipalType;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class GcpKeyPolicyTranslatorTest {

  private static final String ADMIN = "admin@p.iam.gserviceaccount.com";
  private static final String GRANTEE = "grantee@p.iam.gserviceaccount.com";

  @Test
  void capabilitiesDeclareWhatTheTranslatorCanExpress() {
    PolicyCapabilities capabilities = GcpKeyPolicyTranslator.CAPABILITIES;

    assertFalse(capabilities.isSupportsDeny());
    assertFalse(capabilities.isSupportsConditions());
    assertEquals(Set.of(PrincipalType.IDENTITY), capabilities.getSupportedPrincipalTypes());
    assertEquals(
        Set.of(
            KeyAction.ENCRYPT,
            KeyAction.DECRYPT,
            KeyAction.DESCRIBE,
            KeyAction.MANAGE,
            KeyAction.IMPORT_KEY_MATERIAL,
            KeyAction.ALL),
        capabilities.getSupportedActions());
  }

  @Test
  void toBindingsMapsActionsToRolesAndGroupsMembersByRole() {
    KeyPolicy policy =
        KeyPolicy.builder()
            .grant(
                KeyGrant.builder()
                    .principal(KeyPrincipal.of(PrincipalType.IDENTITY, ADMIN))
                    .action(KeyAction.MANAGE)
                    .action(KeyAction.DESCRIBE)
                    .build())
            .grant(
                KeyGrant.builder()
                    .principal(KeyPrincipal.of(PrincipalType.IDENTITY, GRANTEE))
                    .principal(KeyPrincipal.of(PrincipalType.IDENTITY, "user:bob@example.com"))
                    .action(KeyAction.ENCRYPT)
                    .build())
            .grant(
                KeyGrant.builder()
                    .principal(KeyPrincipal.of(PrincipalType.IDENTITY, ADMIN))
                    .action(KeyAction.ENCRYPT)
                    .build())
            .build();

    List<Binding> bindings = GcpKeyPolicyTranslator.toBindings(policy);

    assertEquals(3, bindings.size());
    assertBinding(
        bindings,
        "roles/cloudkms.cryptoKeyEncrypter",
        "serviceAccount:" + GRANTEE,
        "user:bob@example.com",
        "serviceAccount:" + ADMIN);
    assertBinding(bindings, "roles/cloudkms.viewer", "serviceAccount:" + ADMIN);
    assertBinding(bindings, "roles/cloudkms.admin", "serviceAccount:" + ADMIN);
  }

  @Test
  void toBindingsKeepsPrefixedIdentifiersVerbatim() {
    KeyPolicy policy =
        KeyPolicy.builder()
            .grant(
                KeyGrant.builder()
                    .principal(KeyPrincipal.of(PrincipalType.IDENTITY, "group:eng@example.com"))
                    .action(KeyAction.DECRYPT)
                    .build())
            .build();

    List<Binding> bindings = GcpKeyPolicyTranslator.toBindings(policy);

    assertBinding(bindings, "roles/cloudkms.cryptoKeyDecrypter", "group:eng@example.com");
  }

  @Test
  void toKeyPolicyAggregatesActionsPerMemberAndStripsServiceAccountPrefix() {
    Policy policy =
        Policy.newBuilder()
            .addBindings(
                Binding.newBuilder()
                    .setRole("roles/cloudkms.cryptoKeyEncrypterDecrypter")
                    .addMembers("serviceAccount:" + GRANTEE)
                    .addMembers("user:bob@example.com"))
            .addBindings(
                Binding.newBuilder()
                    .setRole("roles/cloudkms.admin")
                    .addMembers("serviceAccount:" + GRANTEE))
            .addBindings(
                Binding.newBuilder()
                    .setRole("roles/cloudkms.viewer")
                    .addMembers("user:bob@example.com"))
            .build();

    KeyPolicy result = GcpKeyPolicyTranslator.toKeyPolicy(policy);

    assertEquals(2, result.getGrants().size());
    KeyGrant grantee = result.getGrants().get(0);
    assertEquals(
        List.of(KeyPrincipal.of(PrincipalType.IDENTITY, GRANTEE)), grantee.getPrincipals());
    assertEquals(Set.of(KeyAction.ALL), grantee.getActions());
    KeyGrant bob = result.getGrants().get(1);
    assertEquals(
        List.of(KeyPrincipal.of(PrincipalType.IDENTITY, "user:bob@example.com")),
        bob.getPrincipals());
    assertEquals(
        Set.of(KeyAction.ENCRYPT, KeyAction.DECRYPT, KeyAction.DESCRIBE), bob.getActions());
  }

  @Test
  void toKeyPolicySkipsUnknownRolesConditionalBindingsAndUntypedMembers() {
    Policy policy =
        Policy.newBuilder()
            .addBindings(
                Binding.newBuilder().setRole("roles/owner").addMembers("serviceAccount:" + ADMIN))
            .addBindings(
                Binding.newBuilder()
                    .setRole("roles/cloudkms.admin")
                    .setCondition(Expr.newBuilder().setExpression("true"))
                    .addMembers("serviceAccount:" + ADMIN))
            .addBindings(
                Binding.newBuilder()
                    .setRole("roles/cloudkms.viewer")
                    .addMembers("allUsers")
                    .addMembers("serviceAccount:" + GRANTEE))
            .build();

    KeyPolicy result = GcpKeyPolicyTranslator.toKeyPolicy(policy);

    assertEquals(1, result.getGrants().size());
    assertEquals(
        List.of(KeyPrincipal.of(PrincipalType.IDENTITY, GRANTEE)),
        result.getGrants().get(0).getPrincipals());
    assertEquals(Set.of(KeyAction.DESCRIBE), result.getGrants().get(0).getActions());
  }

  @Test
  void setThenGetRoundTripsInOneGrantPerMemberNormalForm() {
    KeyPolicy original =
        KeyPolicy.builder()
            .grant(
                KeyGrant.builder()
                    .principal(KeyPrincipal.of(PrincipalType.IDENTITY, ADMIN))
                    .action(KeyAction.MANAGE)
                    .action(KeyAction.DESCRIBE)
                    .build())
            .grant(
                KeyGrant.builder()
                    .principal(KeyPrincipal.of(PrincipalType.IDENTITY, GRANTEE))
                    .action(KeyAction.ENCRYPT)
                    .action(KeyAction.DECRYPT)
                    .action(KeyAction.IMPORT_KEY_MATERIAL)
                    .build())
            .build();

    KeyPolicy roundTripped = roundTrip(original);

    assertEquals(
        KeyPolicy.builder()
            .grant(
                KeyGrant.builder()
                    .principal(KeyPrincipal.of(PrincipalType.IDENTITY, ADMIN))
                    .action(KeyAction.MANAGE)
                    .build())
            .grant(
                KeyGrant.builder()
                    .principal(KeyPrincipal.of(PrincipalType.IDENTITY, GRANTEE))
                    .action(KeyAction.ENCRYPT)
                    .action(KeyAction.DECRYPT)
                    .action(KeyAction.IMPORT_KEY_MATERIAL)
                    .build())
            .build(),
        roundTripped);
  }

  @Test
  void toBindingsMapsImportKeyMaterialToTheImporterRole() {
    List<Binding> bindings =
        GcpKeyPolicyTranslator.toBindings(policyOf(KeyAction.IMPORT_KEY_MATERIAL));

    assertEquals(1, bindings.size());
    assertBinding(bindings, "roles/cloudkms.importer", "serviceAccount:" + GRANTEE);
  }

  @Test
  void toBindingsMapsAllToAdminPlusEncrypterDecrypter() {
    List<Binding> bindings = GcpKeyPolicyTranslator.toBindings(policyOf(KeyAction.ALL));

    assertEquals(2, bindings.size());
    assertBinding(bindings, "roles/cloudkms.admin", "serviceAccount:" + GRANTEE);
    assertBinding(
        bindings, "roles/cloudkms.cryptoKeyEncrypterDecrypter", "serviceAccount:" + GRANTEE);
  }

  @Test
  void allReadsBackAsAll() {
    assertEquals(Set.of(KeyAction.ALL), roundTripActions(KeyAction.ALL));
  }

  @Test
  void adminWithSingleEncryptAndDecryptRolesReadsAsAll() {
    assertEquals(
        Set.of(KeyAction.ALL),
        roundTripActions(KeyAction.MANAGE, KeyAction.ENCRYPT, KeyAction.DECRYPT));
  }

  @Test
  void allAbsorbsEveryOtherAction() {
    assertEquals(
        Set.of(KeyAction.ALL),
        roundTripActions(
            KeyAction.ALL, KeyAction.DESCRIBE, KeyAction.IMPORT_KEY_MATERIAL, KeyAction.ENCRYPT));
  }

  @Test
  void manageAbsorbsDescribeAndImportButNotCryptographicUse() {
    assertEquals(
        Set.of(KeyAction.MANAGE),
        roundTripActions(KeyAction.MANAGE, KeyAction.DESCRIBE, KeyAction.IMPORT_KEY_MATERIAL));
    assertEquals(
        Set.of(KeyAction.MANAGE, KeyAction.ENCRYPT),
        roundTripActions(KeyAction.MANAGE, KeyAction.ENCRYPT));
  }

  @Test
  void importerAndViewerAreKeptWithoutAdmin() {
    assertEquals(
        Set.of(KeyAction.DESCRIBE, KeyAction.IMPORT_KEY_MATERIAL),
        roundTripActions(KeyAction.DESCRIBE, KeyAction.IMPORT_KEY_MATERIAL));
  }

  @Test
  void grantIdIsNotKept() {
    KeyPolicy original =
        KeyPolicy.builder()
            .grant(
                KeyGrant.builder()
                    .id("grant-1")
                    .principal(KeyPrincipal.of(PrincipalType.IDENTITY, GRANTEE))
                    .action(KeyAction.DECRYPT)
                    .build())
            .build();

    assertEquals(null, roundTrip(original).getGrants().get(0).getId());
  }

  private static KeyPolicy policyOf(KeyAction... actions) {
    return KeyPolicy.builder()
        .grant(
            KeyGrant.builder()
                .principal(KeyPrincipal.of(PrincipalType.IDENTITY, GRANTEE))
                .actions(Set.of(actions))
                .build())
        .build();
  }

  private static KeyPolicy roundTrip(KeyPolicy policy) {
    return GcpKeyPolicyTranslator.toKeyPolicy(
        Policy.newBuilder().addAllBindings(GcpKeyPolicyTranslator.toBindings(policy)).build());
  }

  private static Set<KeyAction> roundTripActions(KeyAction... actions) {
    return roundTrip(policyOf(actions)).getGrants().get(0).getActions();
  }

  @Test
  void aGrantNamingSeveralPrincipalsReadsBackAsOneGrantPerPrincipal() {
    KeyPolicy original =
        KeyPolicy.builder()
            .grant(
                KeyGrant.builder()
                    .principal(KeyPrincipal.of(PrincipalType.IDENTITY, ADMIN))
                    .principal(KeyPrincipal.of(PrincipalType.IDENTITY, GRANTEE))
                    .action(KeyAction.DECRYPT)
                    .build())
            .build();

    KeyPolicy roundTripped =
        GcpKeyPolicyTranslator.toKeyPolicy(
            Policy.newBuilder()
                .addAllBindings(GcpKeyPolicyTranslator.toBindings(original))
                .build());

    assertEquals(2, roundTripped.getGrants().size());
    assertTrue(
        roundTripped.getGrants().stream()
            .allMatch(grant -> grant.getActions().equals(Set.of(KeyAction.DECRYPT))));
  }

  private static void assertBinding(List<Binding> bindings, String role, String... members) {
    Binding binding =
        bindings.stream().filter(b -> b.getRole().equals(role)).findFirst().orElseThrow();
    assertEquals(List.of(members), binding.getMembersList());
  }
}
