package com.salesforce.multicloudj.kms.ali;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class AliKeyPolicyTranslatorTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final KeyPrincipal ROLE =
      KeyPrincipal.of(PrincipalType.IDENTITY, "acs:ram::1234:role/app");

  @Test
  void writesDocumentShape() throws Exception {
    KeyPolicy policy =
        KeyPolicy.builder()
            .grant(
                KeyGrant.builder()
                    .principal(ROLE)
                    .principal(KeyPrincipal.of(PrincipalType.ACCOUNT, "5678"))
                    .action(KeyAction.ENCRYPT)
                    .effect(Effect.DENY)
                    .build())
            .build();

    JsonNode doc = MAPPER.readTree(AliKeyPolicyTranslator.toPolicyJson(policy));

    assertEquals("1", doc.get("Version").asText());
    JsonNode statement = doc.get("Statement").get(0);
    assertEquals("Deny", statement.get("Effect").asText());
    assertEquals("acs:ram::1234:role/app", statement.at("/Principal/RAM/0").asText());
    assertEquals("acs:ram::5678:*", statement.at("/Principal/RAM/1").asText());
    assertEquals("kms:Encrypt", statement.at("/Action/0").asText());
    assertEquals(1, statement.get("Action").size());
    assertEquals("*", statement.at("/Resource/0").asText());
  }

  @Test
  void manageMapsToAdministrativeActionsOnly() throws Exception {
    KeyPolicy policy =
        KeyPolicy.builder()
            .grant(KeyGrant.builder().principal(ROLE).action(KeyAction.MANAGE).build())
            .build();

    JsonNode actions =
        MAPPER.readTree(AliKeyPolicyTranslator.toPolicyJson(policy)).at("/Statement/0/Action");

    Set<String> names = new HashSet<>();
    actions.forEach(a -> names.add(a.asText()));
    assertTrue(names.contains("kms:SetKeyPolicy"));
    assertTrue(names.contains("kms:DisableKey"));
    for (String name : names) {
      assertFalse(
          name.equals("kms:Encrypt")
              || name.equals("kms:Decrypt")
              || name.startsWith("kms:GenerateDataKey")
              || name.equals("kms:ReEncrypt"),
          name);
    }
  }

  @Test
  void roundTripsAllGrantShapes() {
    KeyPolicy policy =
        KeyPolicy.builder()
            .grant(
                KeyGrant.builder()
                    .principal(ROLE)
                    .action(KeyAction.MANAGE)
                    .action(KeyAction.DESCRIBE)
                    .build())
            .grant(
                KeyGrant.builder()
                    .principal(KeyPrincipal.of(PrincipalType.ACCOUNT, "5678"))
                    .action(KeyAction.ENCRYPT)
                    .action(KeyAction.DECRYPT)
                    .action(KeyAction.GENERATE_DATA_KEY)
                    .action(KeyAction.REENCRYPT)
                    .build())
            .grant(
                KeyGrant.builder()
                    .principal(ROLE)
                    .action(KeyAction.DECRYPT)
                    .effect(Effect.DENY)
                    .build())
            .build();

    KeyPolicy read =
        AliKeyPolicyTranslator.fromPolicyJson(AliKeyPolicyTranslator.toPolicyJson(policy));

    assertEquals(policy, read);
  }

  @Test
  void readsStringValuedFieldsAndAccountRootPrincipal() {
    String json =
        "{\"Version\":\"1\",\"Statement\":[{\"Effect\":\"Allow\","
            + "\"Principal\":{\"RAM\":\"acs:ram::99:root\"},"
            + "\"Action\":\"kms:Decrypt\",\"Resource\":\"*\"}]}";

    KeyPolicy read = AliKeyPolicyTranslator.fromPolicyJson(json);

    assertEquals(1, read.getGrants().size());
    KeyGrant grant = read.getGrants().get(0);
    assertEquals(Effect.ALLOW, grant.getEffect());
    assertEquals(KeyPrincipal.of(PrincipalType.ACCOUNT, "99"), grant.getPrincipals().get(0));
    assertEquals(Set.of(KeyAction.DECRYPT), grant.getActions());
  }

  @Test
  void omitsServicePrincipalsOnRead() {
    String json =
        "{\"Statement\":[{\"Effect\":\"Allow\","
            + "\"Principal\":{\"RAM\":[\"acs:ram::1234:role/app\"],"
            + "\"Service\":[\"ecs.aliyuncs.com\"]},"
            + "\"Action\":[\"kms:Decrypt\"]},"
            + "{\"Effect\":\"Allow\",\"Principal\":{\"Service\":[\"ecs.aliyuncs.com\"]},"
            + "\"Action\":[\"kms:Encrypt\"]}]}";

    KeyPolicy read = AliKeyPolicyTranslator.fromPolicyJson(json);

    assertEquals(1, read.getGrants().size());
    assertEquals(
        List.of(KeyPrincipal.of(PrincipalType.IDENTITY, "acs:ram::1234:role/app")),
        read.getGrants().get(0).getPrincipals());
  }

  @Test
  void ignoresUnknownActionsAndOmitsStatementsWithoutRecognizedActions() {
    String json =
        "{\"Statement\":["
            + "{\"Effect\":\"Allow\",\"Principal\":{\"RAM\":[\"r1\"]},"
            + "\"Action\":[\"kms:Encrypt\",\"kms:ListAliases\"]},"
            + "{\"Effect\":\"Allow\",\"Principal\":{\"RAM\":[\"r2\"]},"
            + "\"Action\":[\"kms:ListAliases\"]}]}";

    KeyPolicy read = AliKeyPolicyTranslator.fromPolicyJson(json);

    assertEquals(1, read.getGrants().size());
    assertEquals(Set.of(KeyAction.ENCRYPT), read.getGrants().get(0).getActions());
  }

  @Test
  void partialAdministrativeActionsDoNotImplyManage() {
    String json =
        "{\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"RAM\":[\"r1\"]},"
            + "\"Action\":[\"kms:DisableKey\"]}]}";

    assertTrue(AliKeyPolicyTranslator.fromPolicyJson(json).getGrants().isEmpty());
  }

  @Test
  void omitsStatementsThatCannotBeExpressedWithoutWideningAccess() {
    String json =
        "{\"Statement\":["
            + "{\"Effect\":\"Allow\",\"Principal\":{\"RAM\":[\"r1\"]},"
            + "\"Action\":[\"kms:Decrypt\"],\"Condition\":{\"StringEquals\":{\"a\":\"b\"}}},"
            + "{\"Effect\":\"Allow\",\"Principal\":{\"RAM\":[\"r1\"]},"
            + "\"NotAction\":[\"kms:Decrypt\"]},"
            + "{\"Effect\":\"Allow\",\"Principal\":{\"RAM\":[\"*\"]},"
            + "\"Action\":[\"kms:Decrypt\"]},"
            + "{\"Effect\":\"Other\",\"Principal\":{\"RAM\":[\"r1\"]},"
            + "\"Action\":[\"kms:Decrypt\"]}]}";

    assertTrue(AliKeyPolicyTranslator.fromPolicyJson(json).getGrants().isEmpty());
  }

  @Test
  void wildcardActionReadsAsAll() {
    for (String wildcard : new String[] {"kms:*", "*"}) {
      String json =
          "{\"Statement\":{\"Effect\":\"Allow\",\"Principal\":{\"RAM\":[\"r1\"]},"
              + "\"Action\":\""
              + wildcard
              + "\"}}";

      KeyPolicy read = AliKeyPolicyTranslator.fromPolicyJson(json);

      assertEquals(Set.of(KeyAction.ALL), read.getGrants().get(0).getActions());
    }
  }

  @Test
  void allWritesOnlyWildcard() throws Exception {
    KeyPolicy policy =
        KeyPolicy.builder()
            .grant(
                KeyGrant.builder()
                    .principal(ROLE)
                    .action(KeyAction.ALL)
                    .action(KeyAction.DECRYPT)
                    .build())
            .build();

    JsonNode actions =
        MAPPER.readTree(AliKeyPolicyTranslator.toPolicyJson(policy)).at("/Statement/0/Action");

    assertEquals(1, actions.size());
    assertEquals("kms:*", actions.get(0).asText());
    assertEquals(
        Set.of(KeyAction.ALL),
        AliKeyPolicyTranslator.fromPolicyJson(AliKeyPolicyTranslator.toPolicyJson(policy))
            .getGrants()
            .get(0)
            .getActions());
  }

  @Test
  void fineGrainedManageActionsWriteTheirNativeActions() throws Exception {
    KeyPolicy policy =
        KeyPolicy.builder()
            .grant(
                KeyGrant.builder()
                    .principal(ROLE)
                    .action(KeyAction.MANAGE_STATE)
                    .action(KeyAction.MANAGE_POLICY)
                    .action(KeyAction.MANAGE_TAGS)
                    .action(KeyAction.IMPORT_KEY_MATERIAL)
                    .build())
            .build();

    Set<String> names = new HashSet<>();
    MAPPER
        .readTree(AliKeyPolicyTranslator.toPolicyJson(policy))
        .at("/Statement/0/Action")
        .forEach(a -> names.add(a.asText()));

    assertEquals(
        Set.of(
            "kms:EnableKey",
            "kms:DisableKey",
            "kms:SetKeyPolicy",
            "kms:GetKeyPolicy",
            "kms:TagResource",
            "kms:UntagResource",
            "kms:GetParametersForImport",
            "kms:ImportKeyMaterial"),
        names);
    assertEquals(
        policy, AliKeyPolicyTranslator.fromPolicyJson(AliKeyPolicyTranslator.toPolicyJson(policy)));
  }

  @Test
  void readDropsActionsSubsumedByAnotherRecognizedAction() {
    String manage =
        "\"kms:EnableKey\",\"kms:DisableKey\",\"kms:ScheduleKeyDeletion\","
            + "\"kms:CancelKeyDeletion\",\"kms:SetKeyPolicy\",\"kms:GetKeyPolicy\","
            + "\"kms:TagResource\",\"kms:UntagResource\"";
    String json =
        "{\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"RAM\":[\"r1\"]},"
            + "\"Action\":["
            + manage
            + "]},{\"Effect\":\"Allow\",\"Principal\":{\"RAM\":[\"r1\"]},"
            + "\"Action\":[\"kms:TagResource\",\"kms:UntagResource\"]}]}";

    KeyPolicy read = AliKeyPolicyTranslator.fromPolicyJson(json);

    assertEquals(Set.of(KeyAction.MANAGE), read.getGrants().get(0).getActions());
    assertEquals(Set.of(KeyAction.MANAGE_TAGS), read.getGrants().get(1).getActions());
  }

  @Test
  void identityPrincipalsMustBeRamIdentifiers() {
    KeyPolicy policy =
        KeyPolicy.builder()
            .grant(
                KeyGrant.builder()
                    .principal(KeyPrincipal.of(PrincipalType.IDENTITY, "ecs.aliyuncs.com"))
                    .action(KeyAction.DECRYPT)
                    .build())
            .build();

    assertThrows(
        UnSupportedOperationException.class, () -> AliKeyPolicyTranslator.toPolicyJson(policy));
  }

  @Test
  void blankPolicyReadsAsEmpty() {
    assertTrue(AliKeyPolicyTranslator.fromPolicyJson(null).getGrants().isEmpty());
    assertTrue(AliKeyPolicyTranslator.fromPolicyJson(" ").getGrants().isEmpty());
  }

  private static KeyCondition context(ConditionOperator operator, String key, String value) {
    return KeyCondition.builder()
        .operator(operator)
        .attribute(ConditionAttribute.ENCRYPTION_CONTEXT)
        .key(key)
        .value(value)
        .build();
  }

  private static KeyGrant conditionalGrant(KeyAction action, KeyCondition... conditions) {
    KeyGrant.KeyGrantBuilder grant = KeyGrant.builder().principal(ROLE).action(action);
    for (KeyCondition condition : conditions) {
      grant.condition(condition);
    }
    return grant.build();
  }

  @Test
  void writesEncryptionContextConditionsWithTheNativeKey() throws Exception {
    KeyPolicy policy =
        KeyPolicy.builder()
            .grant(
                conditionalGrant(
                    KeyAction.GENERATE_DATA_KEY,
                    context(ConditionOperator.STRING_EQUALS, "sf-auth", "token")))
            .build();

    JsonNode condition =
        MAPPER
            .readTree(AliKeyPolicyTranslator.toPolicyJson(policy))
            .at("/Statement/0/Condition/StringEquals/kms:EncryptionContext:sf-auth/0");

    assertEquals("token", condition.asText());
  }

  @Test
  void encryptionContextConditionsRoundTrip() {
    KeyPolicy policy =
        KeyPolicy.builder()
            .grant(
                conditionalGrant(
                    KeyAction.DECRYPT,
                    context(ConditionOperator.STRING_EQUALS, "sf-auth", "token"),
                    context(ConditionOperator.STRING_LIKE, "Project", "proj-*")))
            .build();

    assertEquals(
        policy, AliKeyPolicyTranslator.fromPolicyJson(AliKeyPolicyTranslator.toPolicyJson(policy)));
  }

  @Test
  void rejectsConditionalReEncryption() {
    KeyPolicy policy =
        KeyPolicy.builder()
            .grant(
                conditionalGrant(
                    KeyAction.REENCRYPT,
                    context(ConditionOperator.STRING_EQUALS, "sf-auth", "token")))
            .build();

    assertThrows(
        UnSupportedOperationException.class, () -> AliKeyPolicyTranslator.toPolicyJson(policy));
  }

  @Test
  void rejectsNonStringOperatorsAndDuplicateKeys() {
    KeyPolicy bool =
        KeyPolicy.builder()
            .grant(
                conditionalGrant(
                    KeyAction.DECRYPT, context(ConditionOperator.BOOL, "sf-auth", "true")))
            .build();
    KeyPolicy duplicate =
        KeyPolicy.builder()
            .grant(
                conditionalGrant(
                    KeyAction.DECRYPT,
                    context(ConditionOperator.STRING_EQUALS, "sf-auth", "a"),
                    context(ConditionOperator.STRING_EQUALS, "sf-auth", "b")))
            .build();

    assertThrows(
        UnSupportedOperationException.class, () -> AliKeyPolicyTranslator.toPolicyJson(bool));
    assertThrows(
        UnSupportedOperationException.class, () -> AliKeyPolicyTranslator.toPolicyJson(duplicate));
  }

  @Test
  void dropsStatementsWithConditionsTheModelCannotExpress() {
    String json =
        "{\"Statement\":["
            + "{\"Effect\":\"Allow\",\"Principal\":{\"RAM\":[\"acs:ram::1234:role/app\"]},"
            + "\"Action\":[\"kms:Decrypt\"],"
            + "\"Condition\":{\"IpAddress\":{\"acs:SourceIp\":\"10.0.0.0/8\"}}},"
            + "{\"Effect\":\"Allow\",\"Principal\":{\"RAM\":[\"acs:ram::1234:role/app\"]},"
            + "\"Action\":[\"kms:Decrypt\"],"
            + "\"Condition\":{\"StringEquals\":{\"acs:SourceVpc\":\"vpc-1\"}}}]}";

    assertTrue(AliKeyPolicyTranslator.fromPolicyJson(json).getGrants().isEmpty());
  }
}
