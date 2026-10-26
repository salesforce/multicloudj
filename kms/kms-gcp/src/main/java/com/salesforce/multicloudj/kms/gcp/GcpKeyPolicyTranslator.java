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
 * ALLOW grant per member holding that member's actions in the following normal form:
 *
 * <ul>
 *   <li>A member holding {@code roles/cloudkms.admin} together with both encrypt and decrypt (via
 *       {@code roles/cloudkms.cryptoKeyEncrypterDecrypter} or both single-purpose roles) reads as
 *       {@code {ALL}}. Cloud KMS has no single all-permissions role, so {@code ALL} is written as
 *       the admin role plus the encrypter/decrypter role.
 *   <li>Otherwise an action implied by another action the member holds is dropped: the admin role
 *       already includes import-job use and the viewer permissions, so {@code MANAGE} absorbs
 *       {@code IMPORT_KEY_MATERIAL} and {@code DESCRIBE}.
 *   <li>Anything else reads as the actions of the member's roles, with {@code
 *       roles/cloudkms.cryptoKeyEncrypterDecrypter} read as {@code ENCRYPT} and {@code DECRYPT}.
 * </ul>
 *
 * <p>{@link KeyGrant#getId()} is not kept: IAM bindings carry no identifier.
 */
final class GcpKeyPolicyTranslator {

  static final PolicyCapabilities CAPABILITIES =
      PolicyCapabilities.builder()
          .supportsDeny(false)
          .supportsConditions(false)
          .supportedPrincipalTypes(EnumSet.of(PrincipalType.IDENTITY))
          .supportedActions(
              EnumSet.of(
                  KeyAction.ENCRYPT,
                  KeyAction.DECRYPT,
                  KeyAction.DESCRIBE,
                  KeyAction.MANAGE,
                  KeyAction.IMPORT_KEY_MATERIAL,
                  KeyAction.ALL))
          .build();

  private static final String SERVICE_ACCOUNT_PREFIX = "serviceAccount:";
  private static final String ADMIN_ROLE = "roles/cloudkms.admin";
  private static final String ENCRYPTER_DECRYPTER_ROLE =
      "roles/cloudkms.cryptoKeyEncrypterDecrypter";

  private static final Map<KeyAction, List<String>> ACTION_ROLES = new EnumMap<>(KeyAction.class);
  private static final Map<String, Set<KeyAction>> ROLE_ACTIONS = new LinkedHashMap<>();

  static {
    ACTION_ROLES.put(KeyAction.ENCRYPT, List.of("roles/cloudkms.cryptoKeyEncrypter"));
    ACTION_ROLES.put(KeyAction.DECRYPT, List.of("roles/cloudkms.cryptoKeyDecrypter"));
    ACTION_ROLES.put(KeyAction.DESCRIBE, List.of("roles/cloudkms.viewer"));
    ACTION_ROLES.put(KeyAction.MANAGE, List.of(ADMIN_ROLE));
    ACTION_ROLES.put(KeyAction.IMPORT_KEY_MATERIAL, List.of("roles/cloudkms.importer"));
    ACTION_ROLES.put(KeyAction.ALL, List.of(ADMIN_ROLE, ENCRYPTER_DECRYPTER_ROLE));

    ROLE_ACTIONS.put(ENCRYPTER_DECRYPTER_ROLE, EnumSet.of(KeyAction.ENCRYPT, KeyAction.DECRYPT));
    ROLE_ACTIONS.put("roles/cloudkms.cryptoKeyEncrypter", EnumSet.of(KeyAction.ENCRYPT));
    ROLE_ACTIONS.put("roles/cloudkms.cryptoKeyDecrypter", EnumSet.of(KeyAction.DECRYPT));
    ROLE_ACTIONS.put("roles/cloudkms.viewer", EnumSet.of(KeyAction.DESCRIBE));
    ROLE_ACTIONS.put(ADMIN_ROLE, EnumSet.of(KeyAction.MANAGE));
    ROLE_ACTIONS.put("roles/cloudkms.importer", EnumSet.of(KeyAction.IMPORT_KEY_MATERIAL));
  }

  private GcpKeyPolicyTranslator() {}

  /** Builds the IAM bindings granting each grant's actions to its principals. */
  static List<Binding> toBindings(KeyPolicy policy) {
    Map<String, Set<String>> membersByRole = new LinkedHashMap<>();
    for (KeyGrant grant : policy.getGrants()) {
      for (KeyAction action : new TreeSet<>(grant.getActions())) {
        for (String role : ACTION_ROLES.get(action)) {
          Set<String> members = membersByRole.computeIfAbsent(role, r -> new LinkedHashSet<>());
          for (KeyPrincipal principal : grant.getPrincipals()) {
            members.add(toMember(principal.getIdentifier()));
          }
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
   * Reads an IAM policy as a {@link KeyPolicy} in the normal form described on this class.
   * Conditional bindings, roles outside the KMS roles this model maps, and members without a type
   * prefix (such as {@code allUsers}) cannot be expressed and are omitted.
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
                    .principal(KeyPrincipal.of(PrincipalType.IDENTITY, fromMember(member)))
                    .actions(normalize(actions))
                    .build()));
    return result.build();
  }

  private static Set<KeyAction> normalize(Set<KeyAction> actions) {
    if (actions.contains(KeyAction.MANAGE)) {
      if (actions.contains(KeyAction.ENCRYPT) && actions.contains(KeyAction.DECRYPT)) {
        return EnumSet.of(KeyAction.ALL);
      }
      Set<KeyAction> normalized = EnumSet.copyOf(actions);
      normalized.remove(KeyAction.IMPORT_KEY_MATERIAL);
      normalized.remove(KeyAction.DESCRIBE);
      return normalized;
    }
    return actions;
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
