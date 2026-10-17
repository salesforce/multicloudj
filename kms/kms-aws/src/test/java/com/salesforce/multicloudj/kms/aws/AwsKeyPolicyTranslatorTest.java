package com.salesforce.multicloudj.kms.aws;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class AwsKeyPolicyTranslatorTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String ROLE = "arn:aws:iam::654654370895:role/app";

  private static JsonNode translate(KeyGrant... grants) throws Exception {
    KeyPolicy.KeyPolicyBuilder policy = KeyPolicy.builder();
    for (KeyGrant grant : grants) {
      policy.grant(grant);
    }
    return MAPPER.readTree(AwsKeyPolicyTranslator.toAwsPolicy(policy.build()));
  }

  private static List<String> texts(JsonNode node) {
    List<String> values = new ArrayList<>();
    if (node.isArray()) {
      node.forEach(n -> values.add(n.asText()));
    } else {
      values.add(node.asText());
    }
    return values;
  }

  private static KeyGrant allow(KeyPrincipal principal, KeyAction... actions) {
    KeyGrant.KeyGrantBuilder builder = KeyGrant.builder().principal(principal);
    for (KeyAction action : actions) {
      builder.action(action);
    }
    return builder.build();
  }

  private static void assertRoundTrips(KeyPolicy policy) {
    KeyPolicy read =
        AwsKeyPolicyTranslator.fromAwsPolicy(AwsKeyPolicyTranslator.toAwsPolicy(policy));
    assertEquals(policy, read);
  }

  @Test
  void roleGrantBecomesAllowStatementWithWildcardResource() throws Exception {
    JsonNode doc =
        translate(
            allow(
                KeyPrincipal.of(PrincipalType.IAM_ROLE, ROLE),
                KeyAction.ENCRYPT,
                KeyAction.DECRYPT));

    assertEquals("2012-10-17", doc.get("Version").asText());
    assertEquals(1, doc.get("Statement").size());
    JsonNode statement = doc.get("Statement").get(0);
    assertEquals("Allow", statement.get("Effect").asText());
    assertEquals("*", statement.get("Resource").asText());
    assertEquals(List.of(ROLE), texts(statement.get("Principal").get("AWS")));
    assertEquals(List.of("kms:Encrypt", "kms:Decrypt"), texts(statement.get("Action")));
    assertFalse(statement.has("Condition"));
  }

  @Test
  void denyEffectIsWritten() throws Exception {
    KeyGrant grant =
        KeyGrant.builder()
            .principal(KeyPrincipal.of(PrincipalType.IAM_ROLE, ROLE))
            .action(KeyAction.DECRYPT)
            .effect(Effect.DENY)
            .build();

    assertEquals("Deny", translate(grant).get("Statement").get(0).get("Effect").asText());
  }

  @Test
  void serviceAndAccountPrincipals() throws Exception {
    KeyGrant grant =
        KeyGrant.builder()
            .principal(KeyPrincipal.of(PrincipalType.ACCOUNT, "123456789012"))
            .principal(KeyPrincipal.of(PrincipalType.SERVICE_ROLE, "logs.amazonaws.com"))
            .action(KeyAction.DESCRIBE)
            .build();

    JsonNode statements = translate(grant).get("Statement");

    assertEquals(2, statements.size());
    assertEquals(
        List.of("arn:aws:iam::123456789012:root"),
        texts(statements.get(0).get("Principal").get("AWS")));
    assertEquals(
        List.of("logs.amazonaws.com"), texts(statements.get(1).get("Principal").get("Service")));
    assertEquals(List.of("kms:DescribeKey"), texts(statements.get(0).get("Action")));
  }

  @Test
  void tenantPrincipalIsOrganizationCondition() throws Exception {
    JsonNode statement =
        translate(allow(KeyPrincipal.of(PrincipalType.TENANT, "o-abc123"), KeyAction.ENCRYPT))
            .get("Statement")
            .get(0);

    assertEquals("*", statement.get("Principal").get("AWS").asText());
    assertEquals(
        List.of("o-abc123"),
        texts(statement.get("Condition").get("StringEquals").get("aws:PrincipalOrgID")));
  }

  @Test
  void wildcardPatternIsPrincipalArnCondition() throws Exception {
    JsonNode statement =
        translate(
                allow(
                    KeyPrincipal.of(PrincipalType.WILDCARD, "arn:aws:iam::1:role/team-*"),
                    KeyAction.DECRYPT))
            .get("Statement")
            .get(0);

    assertEquals("*", statement.get("Principal").get("AWS").asText());
    assertEquals(
        List.of("arn:aws:iam::1:role/team-*"),
        texts(statement.get("Condition").get("ArnLike").get("aws:PrincipalArn")));
  }

  @Test
  void bareWildcardHasNoCondition() throws Exception {
    JsonNode statement =
        translate(allow(KeyPrincipal.of(PrincipalType.WILDCARD, "*"), KeyAction.DECRYPT))
            .get("Statement")
            .get(0);

    assertEquals("*", statement.get("Principal").get("AWS").asText());
    assertFalse(statement.has("Condition"));
  }

  @Test
  void manageDoesNotGrantCryptographicOperations() throws Exception {
    JsonNode actions =
        translate(allow(KeyPrincipal.of(PrincipalType.IAM_ROLE, ROLE), KeyAction.MANAGE))
            .get("Statement")
            .get(0)
            .get("Action");

    List<String> names = texts(actions);
    assertEquals(AwsKeyPolicyTranslator.MANAGE_ACTIONS, names);
    assertFalse(names.contains("kms:*"));
    assertFalse(names.contains("kms:Encrypt"));
    assertFalse(names.contains("kms:Decrypt"));
    assertFalse(names.contains("kms:GenerateDataKey*"));
  }

  @Test
  void singleActionMappings() throws Exception {
    KeyGrant grant =
        allow(
            KeyPrincipal.of(PrincipalType.IAM_ROLE, ROLE),
            KeyAction.GENERATE_DATA_KEY,
            KeyAction.REENCRYPT,
            KeyAction.CREATE_GRANT,
            KeyAction.SIGN,
            KeyAction.VERIFY);

    assertEquals(
        Set.of(
            "kms:GenerateDataKey*", "kms:ReEncrypt*", "kms:CreateGrant", "kms:Sign", "kms:Verify"),
        Set.copyOf(texts(translate(grant).get("Statement").get(0).get("Action"))));
  }

  @Test
  void conditionOperatorsAreMergedUnderOneOperator() throws Exception {
    KeyGrant grant =
        KeyGrant.builder()
            .principal(KeyPrincipal.of(PrincipalType.IAM_ROLE, ROLE))
            .action(KeyAction.ENCRYPT)
            .condition(
                cond(
                    ConditionOperator.STRING_EQUALS,
                    "kms:ViaService",
                    "s3.us-west-2.amazonaws.com"))
            .condition(cond(ConditionOperator.STRING_EQUALS, "kms:CallerAccount", "123456789012"))
            .condition(cond(ConditionOperator.STRING_NOT_EQUALS, "aws:SourceIp", "10.0.0.1"))
            .condition(cond(ConditionOperator.STRING_LIKE, "kms:EncryptionContext:a", "x*"))
            .condition(cond(ConditionOperator.STRING_NOT_LIKE, "kms:EncryptionContext:b", "y*"))
            .condition(
                cond(ConditionOperator.RESOURCE_NAME_EQUALS, "aws:SourceArn", "arn:aws:s3:::b"))
            .condition(
                cond(ConditionOperator.RESOURCE_NAME_LIKE, "aws:SourceArn", "arn:aws:s3:::*"))
            .condition(cond(ConditionOperator.BOOL, "aws:SecureTransport", "true"))
            .build();

    JsonNode condition = translate(grant).get("Statement").get(0).get("Condition");

    List<String> stringEqualsKeys = new ArrayList<>();
    condition.get("StringEquals").fieldNames().forEachRemaining(stringEqualsKeys::add);
    assertEquals(List.of("kms:ViaService", "kms:CallerAccount"), stringEqualsKeys);
    assertEquals(List.of("10.0.0.1"), texts(condition.get("StringNotEquals").get("aws:SourceIp")));
    assertTrue(condition.get("StringLike").has("kms:EncryptionContext:a"));
    assertTrue(condition.get("StringNotLike").has("kms:EncryptionContext:b"));
    assertTrue(condition.get("ArnEquals").has("aws:SourceArn"));
    assertTrue(condition.get("ArnLike").has("aws:SourceArn"));
    assertEquals(List.of("true"), texts(condition.get("Bool").get("aws:SecureTransport")));
  }

  @Test
  void duplicateOperatorAndKeyIsRejected() {
    KeyGrant grant =
        KeyGrant.builder()
            .principal(KeyPrincipal.of(PrincipalType.IAM_ROLE, ROLE))
            .action(KeyAction.ENCRYPT)
            .condition(cond(ConditionOperator.STRING_EQUALS, "kms:ViaService", "a"))
            .condition(cond(ConditionOperator.STRING_EQUALS, "kms:ViaService", "b"))
            .build();

    assertThrows(
        UnSupportedOperationException.class,
        () -> AwsKeyPolicyTranslator.toAwsPolicy(KeyPolicy.builder().grant(grant).build()));
  }

  @Test
  void conditionOnPrincipalDerivedKeyIsRejected() {
    KeyGrant grant =
        KeyGrant.builder()
            .principal(KeyPrincipal.of(PrincipalType.TENANT, "o-1"))
            .action(KeyAction.ENCRYPT)
            .condition(cond(ConditionOperator.STRING_EQUALS, "aws:PrincipalOrgID", "o-2"))
            .build();

    assertThrows(
        UnSupportedOperationException.class,
        () -> AwsKeyPolicyTranslator.toAwsPolicy(KeyPolicy.builder().grant(grant).build()));
  }

  @Test
  void readsStringAndArrayForms() {
    String document =
        "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\","
            + "\"Principal\":{\"AWS\":\"arn:aws:iam::123456789012:root\"},"
            + "\"Action\":\"kms:Decrypt\",\"Resource\":\"*\","
            + "\"Condition\":{\"StringEquals\":{\"kms:ViaService\":\"s3.amazonaws.com\"}}},"
            + "{\"Effect\":\"Deny\",\"Principal\":{\"AWS\":[\""
            + ROLE
            + "\",\"654654370895\"],"
            + "\"Service\":\"logs.amazonaws.com\"},\"Action\":[\"kms:Encrypt\",\"kms:Sign\"],"
            + "\"Resource\":\"*\"}]}";

    KeyPolicy policy = AwsKeyPolicyTranslator.fromAwsPolicy(document);

    assertEquals(2, policy.getGrants().size());
    KeyGrant first = policy.getGrants().get(0);
    assertEquals(Effect.ALLOW, first.getEffect());
    assertEquals(
        List.of(KeyPrincipal.of(PrincipalType.ACCOUNT, "123456789012")), first.getPrincipals());
    assertEquals(Set.of(KeyAction.DECRYPT), first.getActions());
    assertEquals(
        List.of(cond(ConditionOperator.STRING_EQUALS, "kms:ViaService", "s3.amazonaws.com")),
        first.getConditions());

    KeyGrant second = policy.getGrants().get(1);
    assertEquals(Effect.DENY, second.getEffect());
    assertEquals(
        List.of(
            KeyPrincipal.of(PrincipalType.IAM_ROLE, ROLE),
            KeyPrincipal.of(PrincipalType.ACCOUNT, "654654370895"),
            KeyPrincipal.of(PrincipalType.SERVICE_ROLE, "logs.amazonaws.com")),
        second.getPrincipals());
    assertEquals(Set.of(KeyAction.ENCRYPT, KeyAction.SIGN), second.getActions());
  }

  @Test
  void kmsStarMeansEveryAction() {
    String document =
        "{\"Statement\":[{\"Effect\":\"Allow\","
            + "\"Principal\":{\"AWS\":\"arn:aws:iam::123456789012:root\"},"
            + "\"Action\":\"kms:*\",\"Resource\":\"*\"}]}";

    KeyPolicy policy = AwsKeyPolicyTranslator.fromAwsPolicy(document);

    assertEquals(EnumSet.allOf(KeyAction.class), policy.getGrants().get(0).getActions());
  }

  @Test
  void partialAdminSetIsNotManage() {
    String document =
        "{\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\""
            + ROLE
            + "\"},\"Action\":[\"kms:Create*\",\"kms:Describe*\",\"kms:Decrypt\"],"
            + "\"Resource\":\"*\"}]}";

    KeyPolicy policy = AwsKeyPolicyTranslator.fromAwsPolicy(document);

    assertEquals(Set.of(KeyAction.DECRYPT), policy.getGrants().get(0).getActions());
  }

  @Test
  void wildcardPrincipalsAreRecognisedByTheirNarrowingCondition() {
    String document =
        "{\"Statement\":["
            + "{\"Effect\":\"Allow\",\"Principal\":\"*\",\"Action\":\"kms:Decrypt\","
            + "\"Resource\":\"*\","
            + "\"Condition\":{\"ArnLike\":{\"aws:PrincipalArn\":\"arn:aws:iam::1:role/*\"}}},"
            + "{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"*\"},\"Action\":\"kms:Decrypt\","
            + "\"Resource\":\"*\",\"Condition\":{\"StringEquals\":{\"aws:PrincipalOrgID\":\"o-1\","
            + "\"kms:ViaService\":\"s3.amazonaws.com\"}}},"
            + "{\"Effect\":\"Deny\",\"Principal\":\"*\",\"Action\":\"kms:Decrypt\","
            + "\"Resource\":\"*\"}]}";

    List<KeyGrant> grants = AwsKeyPolicyTranslator.fromAwsPolicy(document).getGrants();

    assertEquals(
        List.of(KeyPrincipal.of(PrincipalType.WILDCARD, "arn:aws:iam::1:role/*")),
        grants.get(0).getPrincipals());
    assertTrue(grants.get(0).getConditions().isEmpty());
    assertEquals(
        List.of(KeyPrincipal.of(PrincipalType.TENANT, "o-1")), grants.get(1).getPrincipals());
    assertEquals(
        List.of(cond(ConditionOperator.STRING_EQUALS, "kms:ViaService", "s3.amazonaws.com")),
        grants.get(1).getConditions());
    assertEquals(
        List.of(KeyPrincipal.of(PrincipalType.WILDCARD, "*")), grants.get(2).getPrincipals());
  }

  @Test
  void unexpressibleStatementsAreOmitted() {
    String document =
        "{\"Statement\":["
            + "{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\""
            + ROLE
            + "\"},"
            + "\"Action\":\"kms:ImportKeyMaterial\",\"Resource\":\"*\"},"
            + "{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\""
            + ROLE
            + "\"},"
            + "\"NotAction\":\"kms:Decrypt\",\"Resource\":\"*\"},"
            + "{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\""
            + ROLE
            + "\"},\"Action\":\"kms:Decrypt\",\"Resource\":\"*\","
            + "\"Condition\":{\"NumericLessThan\":{\"kms:GrantConstraintType\":\"1\"}}},"
            + "{\"Effect\":\"Allow\","
            + "\"Principal\":{\"Federated\":\"cognito-identity.amazonaws.com\"},"
            + "\"Action\":\"kms:Decrypt\",\"Resource\":\"*\"},"
            + "{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\""
            + ROLE
            + "\"},"
            + "\"Action\":[\"kms:Decrypt\",\"kms:ImportKeyMaterial\"],\"Resource\":\"*\"}]}";

    KeyPolicy policy = AwsKeyPolicyTranslator.fromAwsPolicy(document);

    assertEquals(1, policy.getGrants().size());
    assertEquals(Set.of(KeyAction.DECRYPT), policy.getGrants().get(0).getActions());
  }

  @Test
  void invalidJsonIsInvalidArgument() {
    assertThrows(
        InvalidArgumentException.class, () -> AwsKeyPolicyTranslator.fromAwsPolicy("{not json"));
  }

  @Test
  void roundTripOfMixedPolicy() {
    assertRoundTrips(
        KeyPolicy.builder()
            .grant(
                allow(
                    KeyPrincipal.of(PrincipalType.ACCOUNT, "654654370895"),
                    KeyAction.MANAGE,
                    KeyAction.DESCRIBE))
            .grant(
                KeyGrant.builder()
                    .principal(KeyPrincipal.of(PrincipalType.IAM_ROLE, ROLE))
                    .principal(KeyPrincipal.of(PrincipalType.SERVICE_ROLE, "logs.amazonaws.com"))
                    .principal(KeyPrincipal.of(PrincipalType.TENANT, "o-abc123"))
                    .principal(KeyPrincipal.of(PrincipalType.TENANT, "o-def456"))
                    .principal(KeyPrincipal.of(PrincipalType.WILDCARD, "arn:aws:iam::1:role/a-*"))
                    .principal(KeyPrincipal.of(PrincipalType.IAM_ROLE, ROLE + "2"))
                    .action(KeyAction.ENCRYPT)
                    .action(KeyAction.DECRYPT)
                    .action(KeyAction.GENERATE_DATA_KEY)
                    .action(KeyAction.REENCRYPT)
                    .action(KeyAction.CREATE_GRANT)
                    .condition(
                        cond(
                            ConditionOperator.STRING_EQUALS,
                            "kms:ViaService",
                            "s3.us-west-2.amazonaws.com"))
                    .condition(
                        cond(ConditionOperator.STRING_EQUALS, "kms:CallerAccount", "654654370895"))
                    .condition(cond(ConditionOperator.BOOL, "aws:SecureTransport", "true"))
                    .build())
            .grant(
                KeyGrant.builder()
                    .principal(KeyPrincipal.of(PrincipalType.WILDCARD, "*"))
                    .action(KeyAction.SIGN)
                    .action(KeyAction.VERIFY)
                    .effect(Effect.DENY)
                    .condition(
                        cond(
                            ConditionOperator.STRING_NOT_LIKE, "aws:PrincipalTag/team", "a*", "b*"))
                    .build())
            .build());
  }

  @Test
  void roundTripOfEveryActionAndPrincipalType() {
    for (KeyAction action : KeyAction.values()) {
      for (PrincipalType type : PrincipalType.values()) {
        String identifier = type == PrincipalType.ACCOUNT ? "123456789012" : "id-" + type;
        assertRoundTrips(
            KeyPolicy.builder().grant(allow(KeyPrincipal.of(type, identifier), action)).build());
      }
    }
  }

  @Test
  void roundTripOfWildcardRunsOfBothKinds() {
    assertRoundTrips(
        KeyPolicy.builder()
            .grant(
                KeyGrant.builder()
                    .principal(KeyPrincipal.of(PrincipalType.WILDCARD, "*"))
                    .principal(KeyPrincipal.of(PrincipalType.WILDCARD, "arn:aws:iam::1:role/*"))
                    .principal(KeyPrincipal.of(PrincipalType.WILDCARD, "arn:aws:iam::2:role/?x"))
                    .action(KeyAction.DESCRIBE)
                    .build())
            .build());
  }

  private static KeyCondition cond(ConditionOperator operator, String key, String... values) {
    KeyCondition.KeyConditionBuilder builder = KeyCondition.builder().operator(operator).key(key);
    for (String value : values) {
      builder.value(value);
    }
    return builder.build();
  }
}
