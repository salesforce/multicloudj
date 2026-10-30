package com.salesforce.multicloudj.kms.aws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.kms.model.ConditionAttribute;
import com.salesforce.multicloudj.kms.model.ConditionOperator;
import com.salesforce.multicloudj.kms.model.Effect;
import com.salesforce.multicloudj.kms.model.KeyAction;
import com.salesforce.multicloudj.kms.model.KeyCondition;
import com.salesforce.multicloudj.kms.model.KeyGrant;
import com.salesforce.multicloudj.kms.model.KeyPolicy;
import com.salesforce.multicloudj.kms.model.KeyPrincipal;
import com.salesforce.multicloudj.kms.model.PrincipalType;
import java.util.ArrayList;
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
  static final String SOURCE_ARN_KEY = "aws:SourceArn";
  static final String SOURCE_ACCOUNT_KEY = "aws:SourceAccount";
  static final String ENCRYPTION_CONTEXT_KEY_PREFIX = "kms:EncryptionContext:";
  static final String ALL_ACTIONS = "kms:*";

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
          "kms:CancelKeyDeletion",
          "kms:RotateKeyOnDemand");

  /** Native actions that make up each action other than {@link KeyAction#ALL}. */
  private static final Map<KeyAction, List<String>> ACTION_VERBS = new EnumMap<>(KeyAction.class);

  /** The same verbs, lower-cased, since action names are case-insensitive. */
  private static final Map<KeyAction, Set<String>> ACTION_VERB_SETS =
      new EnumMap<>(KeyAction.class);

  private static final Map<ConditionOperator, String> OPERATOR_NAMES =
      new EnumMap<>(ConditionOperator.class);
  private static final Map<String, ConditionOperator> OPERATORS_BY_NAME = new HashMap<>();

  static {
    ACTION_VERBS.put(KeyAction.ENCRYPT, List.of("kms:Encrypt"));
    ACTION_VERBS.put(KeyAction.DECRYPT, List.of("kms:Decrypt"));
    ACTION_VERBS.put(KeyAction.GENERATE_DATA_KEY, List.of("kms:GenerateDataKey*"));
    ACTION_VERBS.put(KeyAction.REENCRYPT, List.of("kms:ReEncrypt*"));
    ACTION_VERBS.put(KeyAction.CREATE_GRANT, List.of("kms:CreateGrant"));
    ACTION_VERBS.put(KeyAction.SIGN, List.of("kms:Sign"));
    ACTION_VERBS.put(KeyAction.VERIFY, List.of("kms:Verify"));
    ACTION_VERBS.put(KeyAction.DESCRIBE, List.of("kms:DescribeKey"));
    ACTION_VERBS.put(KeyAction.MANAGE, MANAGE_ACTIONS);
    ACTION_VERBS.put(KeyAction.MANAGE_STATE, List.of("kms:EnableKey", "kms:DisableKey"));
    ACTION_VERBS.put(
        KeyAction.MANAGE_POLICY,
        List.of("kms:GetKeyPolicy", "kms:PutKeyPolicy", "kms:ListKeyPolicies"));
    ACTION_VERBS.put(
        KeyAction.MANAGE_TAGS,
        List.of("kms:TagResource", "kms:UntagResource", "kms:ListResourceTags"));
    ACTION_VERBS.put(
        KeyAction.IMPORT_KEY_MATERIAL,
        List.of("kms:GetParametersForImport", "kms:ImportKeyMaterial"));
    ACTION_VERBS.put(KeyAction.REPLICATE, List.of("kms:ReplicateKey", "kms:UpdatePrimaryRegion"));
    ACTION_VERBS.forEach(
        (action, verbs) -> {
          Set<String> lowered = new LinkedHashSet<>();
          verbs.forEach(verb -> lowered.add(verb.toLowerCase(Locale.ROOT)));
          ACTION_VERB_SETS.put(action, lowered);
        });

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
      List<List<KeyPrincipal>> runs = splitIntoRuns(grant.getPrincipals());
      if (grant.getId() != null) {
        if (runs.size() > 1) {
          throw new UnSupportedOperationException(
              "Grant "
                  + grant.getId()
                  + " mixes principal kinds that need separate statements, so its id cannot be"
                  + " preserved");
        }
        if (GRANT_SID.matcher(grant.getId()).matches()) {
          throw new UnSupportedOperationException(
              "Grant id " + grant.getId() + " is reserved for generated statement ids");
        }
        statements.add(toStatement(grant, runs.get(0), grant.getId()));
        continue;
      }
      int part = 0;
      for (List<KeyPrincipal> run : runs) {
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
      open = KeyGrant.builder().id(idOf(statement)).effect(parsed.effect);
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
      case ACCOUNT:
        return PrincipalClass.AWS;
      case IDENTITY:
        return principal.getIdentifier().startsWith("arn:")
            ? PrincipalClass.AWS
            : PrincipalClass.SERVICE;
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
        ArrayNode services = principal.putArray("Service");
        run.forEach(p -> services.add(p.getIdentifier()));
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
      addCondition(
          conditions, condition.getOperator(), toNativeKey(condition), condition.getValues());
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

  private static String toNativeKey(KeyCondition condition) {
    switch (condition.getAttribute()) {
      case PRINCIPAL_NAME:
        return PRINCIPAL_ARN_KEY;
      case ENCRYPTION_CONTEXT:
        return ENCRYPTION_CONTEXT_KEY_PREFIX + condition.getKey();
      case SOURCE_RESOURCE:
        return SOURCE_ARN_KEY;
      case SOURCE_ACCOUNT:
        return SOURCE_ACCOUNT_KEY;
      case NATIVE:
      default:
        return condition.getKey();
    }
  }

  private static KeyCondition fromNativeKey(
      ConditionOperator operator, String nativeKey, List<String> values) {
    KeyCondition.KeyConditionBuilder builder =
        KeyCondition.builder().operator(operator).values(values);
    if (nativeKey.equalsIgnoreCase(PRINCIPAL_ARN_KEY)) {
      return builder.attribute(ConditionAttribute.PRINCIPAL_NAME).build();
    }
    if (nativeKey.equalsIgnoreCase(SOURCE_ARN_KEY)) {
      return builder.attribute(ConditionAttribute.SOURCE_RESOURCE).build();
    }
    if (nativeKey.equalsIgnoreCase(SOURCE_ACCOUNT_KEY)) {
      return builder.attribute(ConditionAttribute.SOURCE_ACCOUNT).build();
    }
    if (nativeKey.regionMatches(
        true, 0, ENCRYPTION_CONTEXT_KEY_PREFIX, 0, ENCRYPTION_CONTEXT_KEY_PREFIX.length())) {
      return builder
          .attribute(ConditionAttribute.ENCRYPTION_CONTEXT)
          .key(nativeKey.substring(ENCRYPTION_CONTEXT_KEY_PREFIX.length()))
          .build();
    }
    return builder.attribute(ConditionAttribute.NATIVE).key(nativeKey).build();
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
    if (actions.contains(KeyAction.ALL)) {
      names.add(ALL_ACTIONS);
      return names;
    }
    for (KeyAction action : KeyAction.values()) {
      if (actions.contains(action)) {
        names.addAll(ACTION_VERBS.get(action));
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

  /**
   * The statement's {@code Sid} when it is a caller-chosen grant id rather than a generated one.
   */
  private static String idOf(JsonNode statement) {
    JsonNode sid = statement.get("Sid");
    if (sid == null || sid.isNull() || GRANT_SID.matcher(sid.asText()).matches()) {
      return null;
    }
    return sid.asText();
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
                (key, values) -> parsed.conditions.add(fromNativeKey(operator, key, values))));
    return parsed;
  }

  private static void parseActions(JsonNode node, Set<KeyAction> out) {
    Set<String> lowered = new LinkedHashSet<>();
    textValues(node).forEach(name -> lowered.add(name.toLowerCase(Locale.ROOT)));
    if (lowered.contains(ALL_ACTIONS) || lowered.contains("*")) {
      out.add(KeyAction.ALL);
      return;
    }
    Map<KeyAction, Set<String>> recognized = new EnumMap<>(KeyAction.class);
    ACTION_VERB_SETS.forEach(
        (action, verbs) -> {
          if (lowered.containsAll(verbs)) {
            recognized.put(action, verbs);
          }
        });
    // A statement granting a broad action also contains the verbs of narrower ones; only the
    // broad action is reported.
    for (Map.Entry<KeyAction, Set<String>> entry : recognized.entrySet()) {
      boolean subsumed =
          recognized.values().stream()
              .anyMatch(
                  other ->
                      other.size() > entry.getValue().size()
                          && other.containsAll(entry.getValue()));
      if (!subsumed) {
        out.add(entry.getKey());
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
    for (String service : textValues(node.get("Service"))) {
      out.add(KeyPrincipal.of(PrincipalType.IDENTITY, service));
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
        out.add(KeyPrincipal.of(PrincipalType.IDENTITY, value));
      }
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
