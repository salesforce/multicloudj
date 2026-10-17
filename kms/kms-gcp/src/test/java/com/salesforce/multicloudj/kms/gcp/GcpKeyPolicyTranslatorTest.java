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
    assertEquals(
        Set.of(PrincipalType.IAM_ROLE, PrincipalType.SERVICE_ROLE),
        capabilities.getSupportedPrincipalTypes());
    assertEquals(
        Set.of(KeyAction.ENCRYPT, KeyAction.DECRYPT, KeyAction.DESCRIBE, KeyAction.MANAGE),
        capabilities.getSupportedActions());
  }

  @Test
  void toBindingsMapsActionsToRolesAndGroupsMembersByRole() {
    KeyPolicy policy =
        KeyPolicy.builder()
            .grant(
                KeyGrant.builder()
                    .principal(KeyPrincipal.of(PrincipalType.IAM_ROLE, ADMIN))
                    .action(KeyAction.MANAGE)
                    .action(KeyAction.DESCRIBE)
                    .build())
            .grant(
                KeyGrant.builder()
                    .principal(KeyPrincipal.of(PrincipalType.IAM_ROLE, GRANTEE))
                    .principal(KeyPrincipal.of(PrincipalType.SERVICE_ROLE, "user:bob@example.com"))
                    .action(KeyAction.ENCRYPT)
                    .build())
            .grant(
                KeyGrant.builder()
                    .principal(KeyPrincipal.of(PrincipalType.IAM_ROLE, ADMIN))
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
                    .principal(KeyPrincipal.of(PrincipalType.IAM_ROLE, "group:eng@example.com"))
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
        List.of(KeyPrincipal.of(PrincipalType.IAM_ROLE, GRANTEE)), grantee.getPrincipals());
    assertEquals(
        Set.of(KeyAction.ENCRYPT, KeyAction.DECRYPT, KeyAction.MANAGE), grantee.getActions());
    KeyGrant bob = result.getGrants().get(1);
    assertEquals(
        List.of(KeyPrincipal.of(PrincipalType.IAM_ROLE, "user:bob@example.com")),
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
        List.of(KeyPrincipal.of(PrincipalType.IAM_ROLE, GRANTEE)),
        result.getGrants().get(0).getPrincipals());
    assertEquals(Set.of(KeyAction.DESCRIBE), result.getGrants().get(0).getActions());
  }

  @Test
  void setThenGetRoundTripsInOneGrantPerMemberNormalForm() {
    KeyPolicy original =
        KeyPolicy.builder()
            .grant(
                KeyGrant.builder()
                    .principal(KeyPrincipal.of(PrincipalType.IAM_ROLE, ADMIN))
                    .action(KeyAction.MANAGE)
                    .action(KeyAction.DESCRIBE)
                    .build())
            .grant(
                KeyGrant.builder()
                    .principal(KeyPrincipal.of(PrincipalType.IAM_ROLE, GRANTEE))
                    .action(KeyAction.ENCRYPT)
                    .action(KeyAction.DECRYPT)
                    .build())
            .build();

    Policy stored =
        Policy.newBuilder().addAllBindings(GcpKeyPolicyTranslator.toBindings(original)).build();
    KeyPolicy roundTripped = GcpKeyPolicyTranslator.toKeyPolicy(stored);

    assertEquals(original, roundTripped);
  }

  @Test
  void aGrantNamingSeveralPrincipalsReadsBackAsOneGrantPerPrincipal() {
    KeyPolicy original =
        KeyPolicy.builder()
            .grant(
                KeyGrant.builder()
                    .principal(KeyPrincipal.of(PrincipalType.IAM_ROLE, ADMIN))
                    .principal(KeyPrincipal.of(PrincipalType.IAM_ROLE, GRANTEE))
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
