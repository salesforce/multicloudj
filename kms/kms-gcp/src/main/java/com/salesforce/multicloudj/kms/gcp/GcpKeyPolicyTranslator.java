package com.salesforce.multicloudj.kms.gcp;

import com.google.iam.v1.Binding;
import com.google.iam.v1.Policy;
import com.salesforce.multicloudj.kms.model.KeyAction;
import com.salesforce.multicloudj.kms.model.KeyGrant;
import com.salesforce.multicloudj.kms.model.KeyPolicy;
import com.salesforce.multicloudj.kms.model.KeyPrincipal;
import com.salesforce.multicloudj.kms.model.PolicyCapabilities;
import com.salesforce.multicloudj.kms.model.PrincipalType;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Converts between {@link KeyPolicy} and the IAM policy attached to a Cloud KMS crypto key.
 *
 * <p>IAM policies are role-to-members bindings, so a {@link KeyPolicy} read back from a key has one
 * ALLOW grant per member holding that member's aggregated actions.
 */
final class GcpKeyPolicyTranslator {

  static final PolicyCapabilities CAPABILITIES =
      PolicyCapabilities.builder()
          .supportsDeny(false)
          .supportsConditions(false)
          .supportedPrincipalTypes(EnumSet.of(PrincipalType.IAM_ROLE, PrincipalType.SERVICE_ROLE))
          .supportedActions(
              EnumSet.of(
                  KeyAction.ENCRYPT, KeyAction.DECRYPT, KeyAction.DESCRIBE, KeyAction.MANAGE))
          .build();

  private static final String SERVICE_ACCOUNT_PREFIX = "serviceAccount:";

  private static final Map<KeyAction, String> ACTION_ROLES = new EnumMap<>(KeyAction.class);
  private static final Map<String, Set<KeyAction>> ROLE_ACTIONS = new LinkedHashMap<>();

  static {
    ACTION_ROLES.put(KeyAction.ENCRYPT, "roles/cloudkms.cryptoKeyEncrypter");
    ACTION_ROLES.put(KeyAction.DECRYPT, "roles/cloudkms.cryptoKeyDecrypter");
    ACTION_ROLES.put(KeyAction.DESCRIBE, "roles/cloudkms.viewer");
    ACTION_ROLES.put(KeyAction.MANAGE, "roles/cloudkms.admin");

    ROLE_ACTIONS.put(
        "roles/cloudkms.cryptoKeyEncrypterDecrypter",
        EnumSet.of(KeyAction.ENCRYPT, KeyAction.DECRYPT));
    ROLE_ACTIONS.put("roles/cloudkms.cryptoKeyEncrypter", EnumSet.of(KeyAction.ENCRYPT));
    ROLE_ACTIONS.put("roles/cloudkms.cryptoKeyDecrypter", EnumSet.of(KeyAction.DECRYPT));
    ROLE_ACTIONS.put("roles/cloudkms.viewer", EnumSet.of(KeyAction.DESCRIBE));
    ROLE_ACTIONS.put("roles/cloudkms.admin", EnumSet.of(KeyAction.MANAGE));
  }

  private GcpKeyPolicyTranslator() {}

  /** Builds the IAM bindings granting each grant's actions to its principals. */
  static List<Binding> toBindings(KeyPolicy policy) {
    Map<String, Set<String>> membersByRole = new LinkedHashMap<>();
    for (KeyGrant grant : policy.getGrants()) {
      for (KeyAction action : new TreeSet<>(grant.getActions())) {
        Set<String> members =
            membersByRole.computeIfAbsent(ACTION_ROLES.get(action), role -> new LinkedHashSet<>());
        for (KeyPrincipal principal : grant.getPrincipals()) {
          members.add(toMember(principal.getIdentifier()));
        }
      }
    }
    List<Binding> bindings = new ArrayList<>();
    membersByRole.forEach(
        (role, members) ->
            bindings.add(Binding.newBuilder().setRole(role).addAllMembers(members).build()));
    return bindings;
  }

  /**
   * Reads an IAM policy as a {@link KeyPolicy}. Conditional bindings, roles outside the KMS roles
   * this model maps, and members without a type prefix (such as {@code allUsers}) cannot be
   * expressed and are omitted.
   */
  static KeyPolicy toKeyPolicy(Policy policy) {
    Map<String, Set<KeyAction>> actionsByMember = new LinkedHashMap<>();
    for (Binding binding : policy.getBindingsList()) {
      Set<KeyAction> actions = ROLE_ACTIONS.get(binding.getRole());
      if (binding.hasCondition() || actions == null) {
        continue;
      }
      for (String member : binding.getMembersList()) {
        if (member.indexOf(':') < 0) {
          continue;
        }
        actionsByMember
            .computeIfAbsent(member, m -> EnumSet.noneOf(KeyAction.class))
            .addAll(actions);
      }
    }
    KeyPolicy.KeyPolicyBuilder result = KeyPolicy.builder();
    actionsByMember.forEach(
        (member, actions) ->
            result.grant(
                KeyGrant.builder()
                    .principal(KeyPrincipal.of(PrincipalType.IAM_ROLE, fromMember(member)))
                    .actions(actions)
                    .build()));
    return result.build();
  }

  private static String toMember(String identifier) {
    return identifier.indexOf(':') >= 0 ? identifier : SERVICE_ACCOUNT_PREFIX + identifier;
  }

  private static String fromMember(String member) {
    return member.startsWith(SERVICE_ACCOUNT_PREFIX)
        ? member.substring(SERVICE_ACCOUNT_PREFIX.length())
        : member;
  }
}
