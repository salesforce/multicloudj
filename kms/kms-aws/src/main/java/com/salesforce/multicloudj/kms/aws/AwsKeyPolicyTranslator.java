package com.salesforce.multicloudj.kms.aws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.kms.model.ConditionOperator;
import com.salesforce.multicloudj.kms.model.Effect;
import com.salesforce.multicloudj.kms.model.KeyAction;
import com.salesforce.multicloudj.kms.model.KeyCondition;
import com.salesforce.multicloudj.kms.model.KeyGrant;
import com.salesforce.multicloudj.kms.model.KeyPolicy;
import com.salesforce.multicloudj.kms.model.KeyPrincipal;
import com.salesforce.multicloudj.kms.model.PrincipalType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Converts between the portable {@link KeyPolicy} model and the JSON key policy document stored by
 * AWS KMS.
 *
 * <p>Each grant becomes one statement per run of consecutive principals that need the same
 * principal element and principal-derived condition. Statements produced from one grant share a
 * {@code Sid} of the form {@code Grant<n>Part<m>}, which lets {@link #fromAwsPolicy} merge them
 * back into a single grant.
 */
final class AwsKeyPolicyTranslator {

  static final String POLICY_VERSION = "2012-10-17";
  static final String ORG_ID_KEY = "aws:PrincipalOrgID";
  static final String PRINCIPAL_ARN_KEY = "aws:PrincipalArn";

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Pattern GRANT_SID = Pattern.compile("^Grant(\\d+)Part\\d+$");
  private static final Pattern ACCOUNT_ROOT_ARN =
      Pattern.compile("^arn:aws[a-z-]*:iam::(\\d{12}):root$");
  private static final Pattern ACCOUNT_ID = Pattern.compile("^\\d{12}$");

  /**
   * Key administrator actions. Deliberately excludes the cryptographic operations so that managing
   * a key never implies using it.
   */
  static final List<String> MANAGE_ACTIONS =
      List.of(
          "kms:Create*",
          "kms:Describe*",
          "kms:Enable*",
          "kms:List*",
          "kms:Put*",
          "kms:Update*",
          "kms:Revoke*",
          "kms:Disable*",
          "kms:Get*",
          "kms:Delete*",
          "kms:TagResource",
          "kms:UntagResource",
          "kms:ScheduleKeyDeletion",
          "kms:CancelKeyDeletion");

  private static final Map<KeyAction, String> SINGLE_ACTIONS = new EnumMap<>(KeyAction.class);
  private static final Map<String, KeyAction> ACTIONS_BY_NAME = new HashMap<>();
  private static final Map<ConditionOperator, String> OPERATOR_NAMES =
      new EnumMap<>(ConditionOperator.class);
  private static final Map<String, ConditionOperator> OPERATORS_BY_NAME = new HashMap<>();

  static {
    SINGLE_ACTIONS.put(KeyAction.ENCRYPT, "kms:Encrypt");
    SINGLE_ACTIONS.put(KeyAction.DECRYPT, "kms:Decrypt");
    SINGLE_ACTIONS.put(KeyAction.GENERATE_DATA_KEY, "kms:GenerateDataKey*");
    SINGLE_ACTIONS.put(KeyAction.REENCRYPT, "kms:ReEncrypt*");
    SINGLE_ACTIONS.put(KeyAction.CREATE_GRANT, "kms:CreateGrant");
    SINGLE_ACTIONS.put(KeyAction.SIGN, "kms:Sign");
    SINGLE_ACTIONS.put(KeyAction.VERIFY, "kms:Verify");
    SINGLE_ACTIONS.put(KeyAction.DESCRIBE, "kms:DescribeKey");
    SINGLE_ACTIONS.forEach(
        (action, name) -> ACTIONS_BY_NAME.put(name.toLowerCase(Locale.ROOT), action));

    OPERATOR_NAMES.put(ConditionOperator.STRING_EQUALS, "StringEquals");
    OPERATOR_NAMES.put(ConditionOperator.STRING_NOT_EQUALS, "StringNotEquals");
    OPERATOR_NAMES.put(ConditionOperator.STRING_LIKE, "StringLike");
    OPERATOR_NAMES.put(ConditionOperator.STRING_NOT_LIKE, "StringNotLike");
    OPERATOR_NAMES.put(ConditionOperator.RESOURCE_NAME_EQUALS, "ArnEquals");
    OPERATOR_NAMES.put(ConditionOperator.RESOURCE_NAME_LIKE, "ArnLike");
    OPERATOR_NAMES.put(ConditionOperator.BOOL, "Bool");
    OPERATOR_NAMES.forEach((op, name) -> OPERATORS_BY_NAME.put(name, op));
  }

  private AwsKeyPolicyTranslator() {}

  /** How a principal is written into a statement. */
  private enum PrincipalClass {
    AWS,
    SERVICE,
    TENANT,
    WILDCARD_ALL,
    WILDCARD_PATTERN
  }

  /**
   * Builds the JSON policy document for a policy.
   *
   * @throws UnSupportedOperationException if a grant's conditions cannot be expressed without
   *     changing their meaning
   */
  static String toAwsPolicy(KeyPolicy policy) {
    ArrayNode statements = MAPPER.createArrayNode();
    List<KeyGrant> grants = policy.getGrants();
    for (int g = 0; g < grants.size(); g++) {
      KeyGrant grant = grants.get(g);
      int part = 0;
      for (List<KeyPrincipal> run : splitIntoRuns(grant.getPrincipals())) {
        statements.add(toStatement(grant, run, "Grant" + g + "Part" + part++));
      }
    }
    ObjectNode document = MAPPER.createObjectNode();
    document.put("Version", POLICY_VERSION);
    document.set("Statement", statements);
    try {
      return MAPPER.writeValueAsString(document);
    } catch (JsonProcessingException e) {
      throw new InvalidArgumentException("Failed to serialize key policy", e);
    }
  }

  /**
   * Reads a JSON policy document. Statements the portable model cannot express are omitted.
   *
   * @throws InvalidArgumentException if the document is not valid JSON
   */
  static KeyPolicy fromAwsPolicy(String document) {
    JsonNode root;
    try {
      root = MAPPER.readTree(document);
    } catch (JsonProcessingException e) {
      throw new InvalidArgumentException("Key policy is not valid JSON", e);
    }
    JsonNode statements = root == null ? null : root.get("Statement");
    List<JsonNode> statementList = new ArrayList<>();
    if (statements != null && statements.isArray()) {
      statements.forEach(statementList::add);
    } else if (statements != null && statements.isObject()) {
      statementList.add(statements);
    }

    KeyPolicy.KeyPolicyBuilder policy = KeyPolicy.builder();
    KeyGrant.KeyGrantBuilder open = null;
    String openGroup = null;
    for (JsonNode statement : statementList) {
      ParsedStatement parsed = parseStatement(statement);
      if (parsed == null) {
        continue;
      }
      String group = groupOf(statement);
      if (open != null && group != null && group.equals(openGroup)) {
        parsed.principals.forEach(open::principal);
        continue;
      }
      if (open != null) {
        policy.grant(open.build());
      }
      open = KeyGrant.builder().effect(parsed.effect);
      parsed.principals.forEach(open::principal);
      parsed.actions.forEach(open::action);
      parsed.conditions.forEach(open::condition);
      openGroup = group;
    }
    if (open != null) {
      policy.grant(open.build());
    }
    return policy.build();
  }

  private static List<List<KeyPrincipal>> splitIntoRuns(List<KeyPrincipal> principals) {
    List<List<KeyPrincipal>> runs = new ArrayList<>();
    PrincipalClass current = null;
    for (KeyPrincipal principal : principals) {
      PrincipalClass next = classify(principal);
      if (next != current) {
        runs.add(new ArrayList<>());
        current = next;
      }
      runs.get(runs.size() - 1).add(principal);
    }
    return runs;
  }

  private static PrincipalClass classify(KeyPrincipal principal) {
    switch (principal.getType()) {
      case IAM_ROLE:
      case ACCOUNT:
        return PrincipalClass.AWS;
      case SERVICE_ROLE:
        return PrincipalClass.SERVICE;
      case TENANT:
        return PrincipalClass.TENANT;
      case WILDCARD:
        return "*".equals(principal.getIdentifier())
            ? PrincipalClass.WILDCARD_ALL
            : PrincipalClass.WILDCARD_PATTERN;
      default:
        throw new UnSupportedOperationException(
            "Unsupported principal type " + principal.getType());
    }
  }

  private static ObjectNode toStatement(KeyGrant grant, List<KeyPrincipal> run, String sid) {
    PrincipalClass principalClass = classify(run.get(0));
    ObjectNode statement = MAPPER.createObjectNode();
    statement.put("Sid", sid);
    statement.put("Effect", grant.getEffect() == Effect.DENY ? "Deny" : "Allow");

    ObjectNode principal = statement.putObject("Principal");
    Map<ConditionOperator, Map<String, List<String>>> conditions = new LinkedHashMap<>();
    switch (principalClass) {
      case AWS:
        ArrayNode aws = principal.putArray("AWS");
        run.forEach(p -> aws.add(toAwsPrincipal(p)));
        break;
      case SERVICE:
        ArrayNode service = principal.putArray("Service");
        run.forEach(p -> service.add(p.getIdentifier()));
        break;
      case TENANT:
        principal.put("AWS", "*");
        addCondition(
            conditions,
            ConditionOperator.STRING_EQUALS,
            ORG_ID_KEY,
            run.stream().map(KeyPrincipal::getIdentifier).toList());
        break;
      case WILDCARD_PATTERN:
        principal.put("AWS", "*");
        addCondition(
            conditions,
            ConditionOperator.RESOURCE_NAME_LIKE,
            PRINCIPAL_ARN_KEY,
            run.stream().map(KeyPrincipal::getIdentifier).toList());
        break;
      case WILDCARD_ALL:
      default:
        principal.put("AWS", "*");
        break;
    }

    ArrayNode actions = statement.putArray("Action");
    toActionNames(grant.getActions()).forEach(actions::add);
    statement.put("Resource", "*");

    for (KeyCondition condition : grant.getConditions()) {
      addCondition(conditions, condition.getOperator(), condition.getKey(), condition.getValues());
    }
    if (!conditions.isEmpty()) {
      ObjectNode conditionNode = statement.putObject("Condition");
      conditions.forEach(
          (operator, byKey) -> {
            ObjectNode operatorNode = conditionNode.putObject(OPERATOR_NAMES.get(operator));
            byKey.forEach(
                (key, values) -> {
                  ArrayNode array = operatorNode.putArray(key);
                  values.forEach(array::add);
                });
          });
    }
    return statement;
  }

  private static String toAwsPrincipal(KeyPrincipal principal) {
    if (principal.getType() == PrincipalType.ACCOUNT) {
      return "arn:aws:iam::" + principal.getIdentifier() + ":root";
    }
    return principal.getIdentifier();
  }

  /**
   * Adds a condition. A statement can hold each operator and key pair only once, and listing the
   * pair twice would turn an AND of two conditions into an OR of their values, so duplicates are
   * rejected.
   */
  private static void addCondition(
      Map<ConditionOperator, Map<String, List<String>>> conditions,
      ConditionOperator operator,
      String key,
      List<String> values) {
    Map<String, List<String>> byKey =
        conditions.computeIfAbsent(operator, o -> new LinkedHashMap<>());
    if (byKey.containsKey(key)) {
      throw new UnSupportedOperationException(
          "Key policies cannot express more than one " + operator + " condition on " + key);
    }
    byKey.put(key, values);
  }

  private static Set<String> toActionNames(Set<KeyAction> actions) {
    Set<String> names = new LinkedHashSet<>();
    for (KeyAction action : KeyAction.values()) {
      if (!actions.contains(action)) {
        continue;
      }
      if (action == KeyAction.MANAGE) {
        names.addAll(MANAGE_ACTIONS);
      } else {
        names.add(SINGLE_ACTIONS.get(action));
      }
    }
    return names;
  }

  private static String groupOf(JsonNode statement) {
    JsonNode sid = statement.get("Sid");
    if (sid == null) {
      return null;
    }
    Matcher matcher = GRANT_SID.matcher(sid.asText());
    return matcher.matches() ? matcher.group(1) : null;
  }

  private static final class ParsedStatement {
    Effect effect;
    final List<KeyPrincipal> principals = new ArrayList<>();
    final Set<KeyAction> actions = new LinkedHashSet<>();
    final List<KeyCondition> conditions = new ArrayList<>();
  }

  /** Returns null when the statement cannot be expressed in the portable model. */
  private static ParsedStatement parseStatement(JsonNode statement) {
    if (statement.has("NotAction") || statement.has("NotPrincipal")) {
      return null;
    }
    ParsedStatement parsed = new ParsedStatement();
    JsonNode effect = statement.get("Effect");
    if (effect == null) {
      return null;
    }
    if ("Allow".equalsIgnoreCase(effect.asText())) {
      parsed.effect = Effect.ALLOW;
    } else if ("Deny".equalsIgnoreCase(effect.asText())) {
      parsed.effect = Effect.DENY;
    } else {
      return null;
    }

    parseActions(statement.get("Action"), parsed.actions);
    if (parsed.actions.isEmpty()) {
      return null;
    }

    Map<ConditionOperator, Map<String, List<String>>> conditions = new LinkedHashMap<>();
    if (!parseConditions(statement.get("Condition"), conditions)) {
      return null;
    }
    parsePrincipals(statement.get("Principal"), conditions, parsed.principals);
    if (parsed.principals.isEmpty()) {
      return null;
    }
    conditions.forEach(
        (operator, byKey) ->
            byKey.forEach(
                (key, values) ->
                    parsed.conditions.add(
                        KeyCondition.builder()
                            .operator(operator)
                            .key(key)
                            .values(values)
                            .build())));
    return parsed;
  }

  private static void parseActions(JsonNode node, Set<KeyAction> out) {
    List<String> names = textValues(node);
    Set<String> lowered = new LinkedHashSet<>();
    names.forEach(name -> lowered.add(name.toLowerCase(Locale.ROOT)));
    if (lowered.contains("kms:*") || lowered.contains("*")) {
      out.addAll(Arrays.asList(KeyAction.values()));
      return;
    }
    boolean manage =
        MANAGE_ACTIONS.stream().allMatch(a -> lowered.contains(a.toLowerCase(Locale.ROOT)));
    for (KeyAction action : KeyAction.values()) {
      if (action == KeyAction.MANAGE) {
        if (manage) {
          out.add(action);
        }
      } else if (lowered.contains(SINGLE_ACTIONS.get(action).toLowerCase(Locale.ROOT))) {
        out.add(action);
      }
    }
  }

  /** Returns false when the statement uses an operator the model cannot express. */
  private static boolean parseConditions(
      JsonNode node, Map<ConditionOperator, Map<String, List<String>>> out) {
    if (node == null || node.isNull()) {
      return true;
    }
    Iterator<Map.Entry<String, JsonNode>> operators = node.fields();
    while (operators.hasNext()) {
      Map.Entry<String, JsonNode> operatorEntry = operators.next();
      ConditionOperator operator = OPERATORS_BY_NAME.get(operatorEntry.getKey());
      if (operator == null) {
        return false;
      }
      Map<String, List<String>> byKey = out.computeIfAbsent(operator, o -> new LinkedHashMap<>());
      Iterator<Map.Entry<String, JsonNode>> keys = operatorEntry.getValue().fields();
      while (keys.hasNext()) {
        Map.Entry<String, JsonNode> keyEntry = keys.next();
        byKey.put(keyEntry.getKey(), textValues(keyEntry.getValue()));
      }
    }
    return true;
  }

  private static void parsePrincipals(
      JsonNode node,
      Map<ConditionOperator, Map<String, List<String>>> conditions,
      List<KeyPrincipal> out) {
    if (node == null) {
      return;
    }
    if (node.isTextual()) {
      if ("*".equals(node.asText())) {
        addWildcardPrincipals(conditions, out);
      }
      return;
    }
    for (String value : textValues(node.get("AWS"))) {
      if ("*".equals(value)) {
        addWildcardPrincipals(conditions, out);
        continue;
      }
      Matcher root = ACCOUNT_ROOT_ARN.matcher(value);
      if (root.matches()) {
        out.add(KeyPrincipal.of(PrincipalType.ACCOUNT, root.group(1)));
      } else if (ACCOUNT_ID.matcher(value).matches()) {
        out.add(KeyPrincipal.of(PrincipalType.ACCOUNT, value));
      } else {
        out.add(KeyPrincipal.of(PrincipalType.IAM_ROLE, value));
      }
    }
    for (String value : textValues(node.get("Service"))) {
      out.add(KeyPrincipal.of(PrincipalType.SERVICE_ROLE, value));
    }
  }

  /**
   * A wildcard principal narrowed by {@code aws:PrincipalArn} or {@code aws:PrincipalOrgID} is the
   * pattern or tenant principal; the narrowing condition is consumed so it is not reported twice.
   */
  private static void addWildcardPrincipals(
      Map<ConditionOperator, Map<String, List<String>>> conditions, List<KeyPrincipal> out) {
    List<String> patterns =
        consume(conditions, ConditionOperator.RESOURCE_NAME_LIKE, PRINCIPAL_ARN_KEY);
    if (patterns != null) {
      patterns.forEach(p -> out.add(KeyPrincipal.of(PrincipalType.WILDCARD, p)));
      return;
    }
    List<String> orgIds = consume(conditions, ConditionOperator.STRING_EQUALS, ORG_ID_KEY);
    if (orgIds != null) {
      orgIds.forEach(id -> out.add(KeyPrincipal.of(PrincipalType.TENANT, id)));
      return;
    }
    out.add(KeyPrincipal.of(PrincipalType.WILDCARD, "*"));
  }

  private static List<String> consume(
      Map<ConditionOperator, Map<String, List<String>>> conditions,
      ConditionOperator operator,
      String key) {
    Map<String, List<String>> byKey = conditions.get(operator);
    if (byKey == null || !byKey.containsKey(key)) {
      return null;
    }
    List<String> values = byKey.remove(key);
    if (byKey.isEmpty()) {
      conditions.remove(operator);
    }
    return values;
  }

  private static List<String> textValues(JsonNode node) {
    List<String> values = new ArrayList<>();
    if (node == null || node.isNull()) {
      return values;
    }
    if (node.isArray()) {
      node.forEach(element -> values.add(element.asText()));
    } else {
      values.add(node.asText());
    }
    return values;
  }
}
