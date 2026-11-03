package com.salesforce.multicloudj.sts.driver;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class CustomHeaderLookupTest {

  @Test
  void findsNameDifferingOnlyByCase() {
    Map<String, String> values = Map.of("X-Vault-AWS-IAM-Server-ID", "vault.example.com");

    Assertions.assertEquals(
        "vault.example.com",
        CustomHeaderLookup.find(values, "x-vault-aws-iam-server-id", "parameter"));
  }

  @Test
  void findsExactName() {
    Map<String, Object> values = Map.of("aud", "https://verifier.example");

    Assertions.assertEquals(
        "https://verifier.example", CustomHeaderLookup.find(values, "aud", "JWT claim"));
  }

  @Test
  void expectedNameIsLowercasedToo() {
    Map<String, String> values = Map.of("x-tenant", "t1");

    Assertions.assertEquals("t1", CustomHeaderLookup.find(values, "X-Tenant", "parameter"));
  }

  @Test
  void returnsNullWhenNoNameMatches() {
    Assertions.assertNull(CustomHeaderLookup.find(Map.of("other", "v"), "x-tenant", "parameter"));
  }

  @Test
  void namesEqualAfterLowercasingAreAmbiguous() {
    Map<String, String> values = new LinkedHashMap<>();
    values.put("x-tenant", "a");
    values.put("X-Tenant", "b");

    InvalidArgumentException e =
        Assertions.assertThrows(
            InvalidArgumentException.class,
            () -> CustomHeaderLookup.find(values, "x-tenant", "JWT claim"));
    Assertions.assertTrue(
        e.getMessage().contains("matches more than one JWT claim"), e.getMessage());
  }
}
