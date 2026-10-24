package com.salesforce.multicloudj.kms.ali;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.kms.model.Effect;
import com.salesforce.multicloudj.kms.model.KeyAction;
import com.salesforce.multicloudj.kms.model.KeyGrant;
import com.salesforce.multicloudj.kms.model.KeyPolicy;
import com.salesforce.multicloudj.kms.model.KeyPrincipal;
import com.salesforce.multicloudj.kms.model.PrincipalType;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Converts between the portable {@link KeyPolicy} model and the JSON key policy document accepted
 * by KMS: {@code {"Version":"1","Statement":[{"Effect","Principal","Action","Resource"}]}}.
 */
final class AliKeyPolicyTranslator {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String POLICY_VERSION = "1";
  private static final String RAM_PREFIX = "acs:ram::";

  /** Matches the principal that stands for every identity of an account. */
  private static final Pattern ACCOUNT_PRINCIPAL = Pattern.compile("^acs:ram::([^:]+):(\\*|root)$");

  /**
   * Native actions that make up each portable action. MANAGE only lists administrative actions so
   * that it never confers cryptographic access.
   */
  private static final Map<KeyAction, List<String>> NATIVE_ACTIONS = new EnumMap<>(KeyAction.class);

  static {
    NATIVE_ACTIONS.put(KeyAction.ENCRYPT, List.of("kms:Encrypt"));
    NATIVE_ACTIONS.put(KeyAction.DECRYPT, List.of("kms:Decrypt"));
    NATIVE_ACTIONS.put(
        KeyAction.GENERATE_DATA_KEY,
        List.of("kms:GenerateDataKey", "kms:GenerateDataKeyWithoutPlaintext"));
    NATIVE_ACTIONS.put(KeyAction.REENCRYPT, List.of("kms:ReEncrypt"));
    NATIVE_ACTIONS.put(KeyAction.DESCRIBE, List.of("kms:DescribeKey"));
    NATIVE_ACTIONS.put(
        KeyAction.MANAGE,
        List.of(
            "kms:EnableKey",
            "kms:DisableKey",
            "kms:ScheduleKeyDeletion",
            "kms:CancelKeyDeletion",
            "kms:SetKeyPolicy",
            "kms:GetKeyPolicy",
            "kms:TagResource",
            "kms:UntagResource"));
  }

  private AliKeyPolicyTranslator() {}

  static Set<KeyAction> supportedActions() {
    return EnumSet.copyOf(NATIVE_ACTIONS.keySet());
  }

  static String toPolicyJson(KeyPolicy policy) {
    ObjectNode root = MAPPER.createObjectNode();
    root.put("Version", POLICY_VERSION);
    ArrayNode statements = root.putArray("Statement");
    for (KeyGrant grant : policy.getGrants()) {
      statements.add(toStatement(grant));
    }
    try {
      return MAPPER.writeValueAsString(root);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("failed to serialize key policy", e);
    }
  }

  private static ObjectNode toStatement(KeyGrant grant) {
    ObjectNode statement = MAPPER.createObjectNode();
    statement.put("Effect", grant.getEffect() == Effect.DENY ? "Deny" : "Allow");

    ArrayNode ram = MAPPER.createArrayNode();
    ArrayNode service = MAPPER.createArrayNode();
    for (KeyPrincipal principal : grant.getPrincipals()) {
      switch (principal.getType()) {
        case IAM_ROLE -> ram.add(principal.getIdentifier());
        case ACCOUNT -> ram.add(RAM_PREFIX + principal.getIdentifier() + ":*");
        case SERVICE_ROLE -> service.add(principal.getIdentifier());
        default ->
            throw new InvalidArgumentException("unsupported principal type " + principal.getType());
      }
    }
    ObjectNode principals = statement.putObject("Principal");
    if (!ram.isEmpty()) {
      principals.set("RAM", ram);
    }
    if (!service.isEmpty()) {
      principals.set("Service", service);
    }

    ArrayNode actions = statement.putArray("Action");
    for (KeyAction action : EnumSet.copyOf(grant.getActions())) {
      List<String> natives = NATIVE_ACTIONS.get(action);
      if (natives == null) {
        throw new InvalidArgumentException("unsupported key action " + action);
      }
      natives.forEach(actions::add);
    }
    statement.putArray("Resource").add("*");
    return statement;
  }

  /**
   * Reads a policy document. Statements the portable model cannot express without widening access
   * are dropped: statements with conditions, with {@code NotAction}, or without any recognized
   * principal or action.
   */
  static KeyPolicy fromPolicyJson(String json) {
    KeyPolicy.KeyPolicyBuilder policy = KeyPolicy.builder();
    if (json == null || json.isBlank()) {
      return policy.build();
    }
    JsonNode root;
    try {
      root = MAPPER.readTree(json);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("failed to parse key policy", e);
    }
    JsonNode statements = root.path("Statement");
    if (statements.isObject()) {
      addGrant(policy, statements);
    } else {
      statements.forEach(statement -> addGrant(policy, statement));
    }
    return policy.build();
  }

  private static void addGrant(KeyPolicy.KeyPolicyBuilder policy, JsonNode statement) {
    if (statement.has("NotAction")
        || statement.has("NotPrincipal")
        || (statement.has("Condition") && !statement.get("Condition").isEmpty())) {
      return;
    }
    Effect effect;
    String nativeEffect = statement.path("Effect").asText("");
    if ("allow".equalsIgnoreCase(nativeEffect)) {
      effect = Effect.ALLOW;
    } else if ("deny".equalsIgnoreCase(nativeEffect)) {
      effect = Effect.DENY;
    } else {
      return;
    }

    List<KeyPrincipal> principals = readPrincipals(statement.path("Principal"));
    Set<KeyAction> actions = readActions(statement.path("Action"));
    if (principals.isEmpty() || actions.isEmpty()) {
      return;
    }
    KeyGrant.KeyGrantBuilder grant = KeyGrant.builder().effect(effect);
    principals.forEach(grant::principal);
    actions.forEach(grant::action);
    policy.grant(grant.build());
  }

  private static List<KeyPrincipal> readPrincipals(JsonNode principal) {
    List<KeyPrincipal> principals = new ArrayList<>();
    for (String ram : values(principal.path("RAM"))) {
      Matcher account = ACCOUNT_PRINCIPAL.matcher(ram);
      if (account.matches()) {
        principals.add(KeyPrincipal.of(PrincipalType.ACCOUNT, account.group(1)));
      } else if (!ram.equals("*")) {
        principals.add(KeyPrincipal.of(PrincipalType.IAM_ROLE, ram));
      }
    }
    for (String service : values(principal.path("Service"))) {
      principals.add(KeyPrincipal.of(PrincipalType.SERVICE_ROLE, service));
    }
    return principals;
  }

  private static Set<KeyAction> readActions(JsonNode actionNode) {
    Set<String> nativeActions = new HashSet<>();
    for (String action : values(actionNode)) {
      nativeActions.add(action.toLowerCase(Locale.ROOT));
    }
    boolean all = nativeActions.contains("*") || nativeActions.contains("kms:*");
    Set<KeyAction> actions = EnumSet.noneOf(KeyAction.class);
    for (Map.Entry<KeyAction, List<String>> entry : NATIVE_ACTIONS.entrySet()) {
      boolean granted =
          all
              || entry.getValue().stream()
                  .allMatch(n -> nativeActions.contains(n.toLowerCase(Locale.ROOT)));
      if (granted) {
        actions.add(entry.getKey());
      }
    }
    return actions;
  }

  /** Reads a JSON value that is either a single string or an array of strings. */
  private static List<String> values(JsonNode node) {
    List<String> values = new ArrayList<>();
    if (node.isTextual()) {
      values.add(node.asText());
    } else if (node.isArray()) {
      node.forEach(
          element -> {
            if (element.isTextual()) {
              values.add(element.asText());
            }
          });
    }
    return values;
  }
}
