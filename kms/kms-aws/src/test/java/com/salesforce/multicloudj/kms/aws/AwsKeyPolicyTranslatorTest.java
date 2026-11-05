package com.salesforce.multicloudj.kms.aws;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
                KeyPrincipal.of(PrincipalType.IDENTITY, ROLE),
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
            .principal(KeyPrincipal.of(PrincipalType.IDENTITY, ROLE))
            .action(KeyAction.DECRYPT)
            .effect(Effect.DENY)
            .build();

    assertEquals("Deny", translate(grant).get("Statement").get(0).get("Effect").asText());
  }

  @Test
  void accountPrincipalIsAccountRootArn() throws Exception {
    JsonNode statements =
        translate(allow(KeyPrincipal.of(PrincipalType.ACCOUNT, "123456789012"), KeyAction.DESCRIBE))
            .get("Statement");

    assertEquals(1, statements.size());
    assertEquals(
        List.of("arn:aws:iam::123456789012:root"),
        texts(statements.get(0).get("Principal").get("AWS")));
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
        translate(allow(KeyPrincipal.of(PrincipalType.IDENTITY, ROLE), KeyAction.MANAGE))
            .get("Statement")
            .get(0)
            .get("Action");

    List<String> names = texts(actions);
    assertEquals(AwsKeyPolicyTranslator.MANAGE_ACTIONS, names);
    assertFalse(names.contains("kms:*"));
    assertFalse(names.contains("kms:Encrypt"));
    assertFalse(names.contains("kms:Decrypt"));
    assertTrue(names.contains("kms:RotateKeyOnDemand"));
    assertFalse(names.contains("kms:GenerateDataKey*"));
  }

  @Test
  void singleActionMappings() throws Exception {
    KeyGrant grant =
        allow(
            KeyPrincipal.of(PrincipalType.IDENTITY, ROLE),
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
            .principal(KeyPrincipal.of(PrincipalType.IDENTITY, ROLE))
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
            .principal(KeyPrincipal.of(PrincipalType.IDENTITY, ROLE))
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
            KeyPrincipal.of(PrincipalType.IDENTITY, "logs.amazonaws.com"),
            KeyPrincipal.of(PrincipalType.IDENTITY, ROLE),
            KeyPrincipal.of(PrincipalType.ACCOUNT, "654654370895")),
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

    assertEquals(EnumSet.of(KeyAction.ALL), policy.getGrants().get(0).getActions());
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
                    .principal(KeyPrincipal.of(PrincipalType.IDENTITY, ROLE))
                    .principal(KeyPrincipal.of(PrincipalType.TENANT, "o-abc123"))
                    .principal(KeyPrincipal.of(PrincipalType.TENANT, "o-def456"))
                    .principal(KeyPrincipal.of(PrincipalType.WILDCARD, "arn:aws:iam::1:role/a-*"))
                    .principal(KeyPrincipal.of(PrincipalType.IDENTITY, ROLE + "2"))
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

  private static final String ACCT = "111122223333";
  private static final String SKB_ROLE = "arn:aws:iam::" + ACCT + ":role/skbKmsRole";
  private static final String BROKER_ROLE = "arn:aws:iam::" + ACCT + ":role/skbBroker";

  private static KeyCondition attr(
      ConditionOperator operator, ConditionAttribute attribute, String key, String... values) {
    KeyCondition.KeyConditionBuilder builder =
        KeyCondition.builder().operator(operator).attribute(attribute).key(key);
    for (String value : values) {
      builder.value(value);
    }
    return builder.build();
  }

  private static KeyPolicy skbPolicy() {
    return KeyPolicy.builder()
        .grant(
            KeyGrant.builder()
                .id("EnableIamUserPermissions")
                .principal(KeyPrincipal.of(PrincipalType.ACCOUNT, ACCT))
                .action(KeyAction.ALL)
                .build())
        .grant(
            KeyGrant.builder()
                .id("AllowSkbKeyAdministrators")
                .principal(KeyPrincipal.of(PrincipalType.IDENTITY, SKB_ROLE))
                .action(KeyAction.DESCRIBE)
                .action(KeyAction.MANAGE_STATE)
                .action(KeyAction.MANAGE_POLICY)
                .action(KeyAction.MANAGE_TAGS)
                .action(KeyAction.IMPORT_KEY_MATERIAL)
                .action(KeyAction.REPLICATE)
                .build())
        .grant(
            KeyGrant.builder()
                .id("AllowSalesforceDataCloud1")
                .principal(KeyPrincipal.of(PrincipalType.ACCOUNT, ACCT))
                .condition(
                    attr(
                        ConditionOperator.RESOURCE_NAME_LIKE,
                        ConditionAttribute.PRINCIPAL_NAME,
                        null,
                        "arn:aws:iam::" + ACCT + ":role/sfDataCloud*"))
                .action(KeyAction.CREATE_GRANT)
                .action(KeyAction.DECRYPT)
                .action(KeyAction.DESCRIBE)
                .action(KeyAction.ENCRYPT)
                .action(KeyAction.GENERATE_DATA_KEY)
                .action(KeyAction.REENCRYPT)
                .build())
        .grant(
            KeyGrant.builder()
                .id("AllowS3InventoryEncryptionForArchivedBuckets")
                .principal(KeyPrincipal.of(PrincipalType.IDENTITY, "s3.amazonaws.com"))
                .action(KeyAction.GENERATE_DATA_KEY)
                .condition(
                    attr(
                        ConditionOperator.RESOURCE_NAME_LIKE,
                        ConditionAttribute.SOURCE_RESOURCE,
                        null,
                        "arn:aws:s3:::sf-archive-*"))
                .condition(
                    attr(
                        ConditionOperator.STRING_EQUALS,
                        ConditionAttribute.SOURCE_ACCOUNT,
                        null,
                        ACCT))
                .build())
        .grant(
            KeyGrant.builder()
                .id("AllowSalesforceShieldKeyBroker1")
                .principal(KeyPrincipal.of(PrincipalType.IDENTITY, BROKER_ROLE))
                .action(KeyAction.GENERATE_DATA_KEY)
                .action(KeyAction.DECRYPT)
                .condition(
                    attr(
                        ConditionOperator.STRING_EQUALS,
                        ConditionAttribute.ENCRYPTION_CONTEXT,
                        "sf-auth",
                        "token"))
                .build())
        .build();
  }

  @Test
  void idBearingPolicyUsesIdsAsSidsAndNativeShapes() throws Exception {
    KeyPolicy policy = skbPolicy();
    JsonNode statements =
        MAPPER.readTree(AwsKeyPolicyTranslator.toAwsPolicy(policy)).get("Statement");

    assertEquals(5, statements.size());
    JsonNode root = statements.get(0);
    assertEquals("EnableIamUserPermissions", root.get("Sid").asText());
    assertEquals(
        List.of("arn:aws:iam::" + ACCT + ":root"), texts(root.get("Principal").get("AWS")));
    assertEquals(List.of("kms:*"), texts(root.get("Action")));

    JsonNode admins = statements.get(1);
    assertEquals("AllowSkbKeyAdministrators", admins.get("Sid").asText());
    assertEquals(
        List.of(
            "kms:DescribeKey",
            "kms:EnableKey",
            "kms:DisableKey",
            "kms:GetKeyPolicy",
            "kms:PutKeyPolicy",
            "kms:ListKeyPolicies",
            "kms:TagResource",
            "kms:UntagResource",
            "kms:ListResourceTags",
            "kms:GetParametersForImport",
            "kms:ImportKeyMaterial",
            "kms:ReplicateKey",
            "kms:UpdatePrimaryRegion"),
        texts(admins.get("Action")));

    JsonNode dataCloud = statements.get(2);
    assertEquals("AllowSalesforceDataCloud1", dataCloud.get("Sid").asText());
    assertEquals(
        List.of("arn:aws:iam::" + ACCT + ":root"), texts(dataCloud.get("Principal").get("AWS")));
    assertEquals(
        List.of("arn:aws:iam::" + ACCT + ":role/sfDataCloud*"),
        texts(dataCloud.get("Condition").get("ArnLike").get("aws:PrincipalArn")));

    JsonNode s3 = statements.get(3);
    assertEquals("AllowS3InventoryEncryptionForArchivedBuckets", s3.get("Sid").asText());
    assertEquals(List.of("s3.amazonaws.com"), texts(s3.get("Principal").get("Service")));
    assertFalse(s3.get("Principal").has("AWS"));
    assertEquals(
        List.of("arn:aws:s3:::sf-archive-*"),
        texts(s3.get("Condition").get("ArnLike").get("aws:SourceArn")));
    assertEquals(
        List.of(ACCT), texts(s3.get("Condition").get("StringEquals").get("aws:SourceAccount")));

    JsonNode broker = statements.get(4);
    assertEquals("AllowSalesforceShieldKeyBroker1", broker.get("Sid").asText());
    assertEquals(List.of(BROKER_ROLE), texts(broker.get("Principal").get("AWS")));
    assertEquals(
        List.of("token"),
        texts(broker.get("Condition").get("StringEquals").get("kms:EncryptionContext:sf-auth")));

    assertRoundTrips(policy);
  }

  @Test
  void accountWithPrincipalNameConditionIsNotReadAsWildcard() {
    KeyGrant grant =
        KeyGrant.builder()
            .principal(KeyPrincipal.of(PrincipalType.ACCOUNT, ACCT))
            .action(KeyAction.DECRYPT)
            .condition(
                attr(
                    ConditionOperator.RESOURCE_NAME_LIKE,
                    ConditionAttribute.PRINCIPAL_NAME,
                    null,
                    "arn:aws:iam::" + ACCT + ":role/x*"))
            .build();

    assertRoundTrips(KeyPolicy.builder().grant(grant).build());
  }

  @Test
  void userConditionOnPrincipalArnAlongsideWildcardPatternIsRejected() {
    KeyGrant grant =
        KeyGrant.builder()
            .principal(KeyPrincipal.of(PrincipalType.WILDCARD, "arn:aws:iam::1:role/*"))
            .action(KeyAction.DECRYPT)
            .condition(
                attr(
                    ConditionOperator.RESOURCE_NAME_LIKE,
                    ConditionAttribute.PRINCIPAL_NAME,
                    null,
                    "arn:aws:iam::1:role/a*"))
            .build();

    assertThrows(
        UnSupportedOperationException.class,
        () -> AwsKeyPolicyTranslator.toAwsPolicy(KeyPolicy.builder().grant(grant).build()));
  }

  @Test
  void nativeKeysAreRecognisedCaseInsensitivelyOnRead() {
    String document =
        "{\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\""
            + ROLE
            + "\"},\"Action\":\"kms:Decrypt\",\"Resource\":\"*\","
            + "\"Condition\":{\"StringEquals\":{\"AWS:sourceaccount\":\"1\","
            + "\"KMS:encryptioncontext:MyKey\":\"v\",\"kms:ViaService\":\"s3\"},"
            + "\"ArnLike\":{\"AWS:SOURCEARN\":\"arn:*\",\"AWS:principalarn\":\"p*\"}}}]}";

    List<KeyCondition> conditions =
        AwsKeyPolicyTranslator.fromAwsPolicy(document).getGrants().get(0).getConditions();

    assertEquals(
        List.of(
            attr(ConditionOperator.STRING_EQUALS, ConditionAttribute.SOURCE_ACCOUNT, null, "1"),
            attr(
                ConditionOperator.STRING_EQUALS,
                ConditionAttribute.ENCRYPTION_CONTEXT,
                "MyKey",
                "v"),
            attr(
                ConditionOperator.STRING_EQUALS, ConditionAttribute.NATIVE, "kms:ViaService", "s3"),
            attr(
                ConditionOperator.RESOURCE_NAME_LIKE,
                ConditionAttribute.SOURCE_RESOURCE,
                null,
                "arn:*"),
            attr(
                ConditionOperator.RESOURCE_NAME_LIKE,
                ConditionAttribute.PRINCIPAL_NAME,
                null,
                "p*")),
        conditions);
  }

  @Test
  void allActionWinsOverOtherActionsOnWrite() throws Exception {
    JsonNode actions =
        translate(
                allow(
                    KeyPrincipal.of(PrincipalType.IDENTITY, ROLE),
                    KeyAction.ENCRYPT,
                    KeyAction.ALL))
            .get("Statement")
            .get(0)
            .get("Action");

    assertEquals(List.of("kms:*"), texts(actions));
  }

  @Test
  void bareStarActionMeansAll() {
    String document =
        "{\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\""
            + ROLE
            + "\"},\"Action\":[\"*\",\"kms:Decrypt\"],\"Resource\":\"*\"}]}";

    assertEquals(
        Set.of(KeyAction.ALL),
        AwsKeyPolicyTranslator.fromAwsPolicy(document).getGrants().get(0).getActions());
  }

  @Test
  void manageStatementDoesNotReportItsSubActions() {
    KeyPolicy policy =
        KeyPolicy.builder()
            .grant(allow(KeyPrincipal.of(PrincipalType.IDENTITY, ROLE), KeyAction.MANAGE))
            .build();

    assertRoundTrips(policy);
  }

  @Test
  void narrowerActionIsDroppedWhenItsVerbsAreContainedInABroaderOne() {
    String document =
        "{\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\""
            + ROLE
            + "\"},\"Action\":[\"kms:EnableKey\",\"kms:DisableKey\",\"kms:GetKeyPolicy\","
            + "\"kms:PutKeyPolicy\",\"kms:ListKeyPolicies\"],\"Resource\":\"*\"}]}";

    assertEquals(
        Set.of(KeyAction.MANAGE_STATE, KeyAction.MANAGE_POLICY),
        AwsKeyPolicyTranslator.fromAwsPolicy(document).getGrants().get(0).getActions());
  }

  @Test
  void compositeActionNeedsEveryVerb() {
    String document =
        "{\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\""
            + ROLE
            + "\"},\"Action\":[\"KMS:replicatekey\",\"kms:Encrypt\"],\"Resource\":\"*\"}]}";

    assertEquals(
        Set.of(KeyAction.ENCRYPT),
        AwsKeyPolicyTranslator.fromAwsPolicy(document).getGrants().get(0).getActions());
  }

  @Test
  void everyActionCombinationOfNewActionsRoundTrips() {
    List<KeyAction> composites =
        List.of(
            KeyAction.MANAGE,
            KeyAction.MANAGE_STATE,
            KeyAction.MANAGE_POLICY,
            KeyAction.MANAGE_TAGS,
            KeyAction.IMPORT_KEY_MATERIAL,
            KeyAction.REPLICATE,
            KeyAction.DESCRIBE);
    for (int mask = 1; mask < (1 << composites.size()); mask++) {
      KeyGrant.KeyGrantBuilder grant =
          KeyGrant.builder().principal(KeyPrincipal.of(PrincipalType.IDENTITY, ROLE));
      for (int i = 0; i < composites.size(); i++) {
        if ((mask & (1 << i)) != 0) {
          grant.action(composites.get(i));
        }
      }
      assertRoundTrips(KeyPolicy.builder().grant(grant.build()).build());
    }
  }

  @Test
  void generatedSidsAreUsedWithoutIdAndReadBackWithoutId() throws Exception {
    KeyGrant grant = allow(KeyPrincipal.of(PrincipalType.IDENTITY, ROLE), KeyAction.DECRYPT);

    assertEquals("Grant0Part0", translate(grant).get("Statement").get(0).get("Sid").asText());
    assertNull(
        AwsKeyPolicyTranslator.fromAwsPolicy(
                AwsKeyPolicyTranslator.toAwsPolicy(KeyPolicy.builder().grant(grant).build()))
            .getGrants()
            .get(0)
            .getId());
  }

  @Test
  void grantIdNeedingMultipleStatementsIsRejected() {
    KeyGrant grant =
        KeyGrant.builder()
            .id("Mixed")
            .principal(KeyPrincipal.of(PrincipalType.IDENTITY, ROLE))
            .principal(KeyPrincipal.of(PrincipalType.IDENTITY, "s3.amazonaws.com"))
            .action(KeyAction.DECRYPT)
            .build();

    assertThrows(
        UnSupportedOperationException.class,
        () -> AwsKeyPolicyTranslator.toAwsPolicy(KeyPolicy.builder().grant(grant).build()));
  }

  @Test
  void grantIdMatchingGeneratedSidPatternIsRejected() {
    KeyGrant grant =
        KeyGrant.builder()
            .id("Grant3Part0")
            .principal(KeyPrincipal.of(PrincipalType.IDENTITY, ROLE))
            .action(KeyAction.DECRYPT)
            .build();

    assertThrows(
        UnSupportedOperationException.class,
        () -> AwsKeyPolicyTranslator.toAwsPolicy(KeyPolicy.builder().grant(grant).build()));
  }

  @Test
  void identityWithoutArnPrefixIsServicePrincipalInItsOwnStatement() throws Exception {
    KeyGrant grant =
        KeyGrant.builder()
            .principal(KeyPrincipal.of(PrincipalType.IDENTITY, ROLE))
            .principal(KeyPrincipal.of(PrincipalType.IDENTITY, "logs.amazonaws.com"))
            .action(KeyAction.ENCRYPT)
            .build();

    JsonNode statements = translate(grant).get("Statement");

    assertEquals(2, statements.size());
    assertEquals(List.of(ROLE), texts(statements.get(0).get("Principal").get("AWS")));
    assertEquals(
        List.of("logs.amazonaws.com"), texts(statements.get(1).get("Principal").get("Service")));
    assertRoundTrips(KeyPolicy.builder().grant(grant).build());
  }

  @Test
  void nonGeneratedSidBecomesGrantIdAndGeneratedSidsStillMerge() {
    String document =
        "{\"Statement\":["
            + "{\"Sid\":\"Custom\",\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\""
            + ROLE
            + "\"},\"Action\":\"kms:Decrypt\",\"Resource\":\"*\"},"
            + "{\"Sid\":\"Grant1Part0\",\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\""
            + ROLE
            + "\"},\"Action\":\"kms:Decrypt\",\"Resource\":\"*\"},"
            + "{\"Sid\":\"Grant1Part1\",\"Effect\":\"Allow\","
            + "\"Principal\":{\"Service\":\"s3.amazonaws.com\"},"
            + "\"Action\":\"kms:Decrypt\",\"Resource\":\"*\"}]}";

    List<KeyGrant> grants = AwsKeyPolicyTranslator.fromAwsPolicy(document).getGrants();

    assertEquals(2, grants.size());
    assertEquals("Custom", grants.get(0).getId());
    assertNull(grants.get(1).getId());
    assertEquals(2, grants.get(1).getPrincipals().size());
  }

  private static KeyCondition cond(ConditionOperator operator, String key, String... values) {
    KeyCondition.KeyConditionBuilder builder =
        KeyCondition.builder().operator(operator).attribute(ConditionAttribute.NATIVE).key(key);
    for (String value : values) {
      builder.value(value);
    }
    return builder.build();
  }
}
