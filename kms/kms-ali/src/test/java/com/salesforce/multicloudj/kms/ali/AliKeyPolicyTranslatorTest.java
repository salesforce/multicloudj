package com.salesforce.multicloudj.kms.ali;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.salesforce.multicloudj.kms.model.Effect;
import com.salesforce.multicloudj.kms.model.KeyAction;
import com.salesforce.multicloudj.kms.model.KeyGrant;
import com.salesforce.multicloudj.kms.model.KeyPolicy;
import com.salesforce.multicloudj.kms.model.KeyPrincipal;
import com.salesforce.multicloudj.kms.model.PrincipalType;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class AliKeyPolicyTranslatorTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final KeyPrincipal ROLE =
      KeyPrincipal.of(PrincipalType.IAM_ROLE, "acs:ram::1234:role/app");

  @Test
  void writesDocumentShape() throws Exception {
    KeyPolicy policy =
        KeyPolicy.builder()
            .grant(
                KeyGrant.builder()
                    .principal(ROLE)
                    .principal(KeyPrincipal.of(PrincipalType.ACCOUNT, "5678"))
                    .principal(KeyPrincipal.of(PrincipalType.SERVICE_ROLE, "ecs.aliyuncs.com"))
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
    assertEquals("ecs.aliyuncs.com", statement.at("/Principal/Service/0").asText());
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
                    .principal(KeyPrincipal.of(PrincipalType.SERVICE_ROLE, "ecs.aliyuncs.com"))
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
  void wildcardActionGrantsEveryAction() {
    String json =
        "{\"Statement\":{\"Effect\":\"Allow\",\"Principal\":{\"RAM\":[\"r1\"]},"
            + "\"Action\":\"kms:*\"}}";

    KeyPolicy read = AliKeyPolicyTranslator.fromPolicyJson(json);

    assertEquals(AliKeyPolicyTranslator.supportedActions(), read.getGrants().get(0).getActions());
  }

  @Test
  void blankPolicyReadsAsEmpty() {
    assertTrue(AliKeyPolicyTranslator.fromPolicyJson(null).getGrants().isEmpty());
    assertTrue(AliKeyPolicyTranslator.fromPolicyJson(" ").getGrants().isEmpty());
  }
}
