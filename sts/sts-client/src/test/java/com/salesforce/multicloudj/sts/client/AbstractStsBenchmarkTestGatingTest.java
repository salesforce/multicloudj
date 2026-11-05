package com.salesforce.multicloudj.sts.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the capability-driven benchmark gating in {@link AbstractStsBenchmarkTest}. These
 * verify that {@code computeExcludes} drops exactly the unexercisable benchmarks for each
 * provider/token combination, without needing a live JMH run.
 */
class AbstractStsBenchmarkTestGatingTest {

  private static final String ACCESS_TOKEN = ".*benchmarkGetAccessToken.*";
  private static final String WEB_IDENTITY = ".*benchmarkGetAssumeRoleWithWebIdentity.*";

  /** Minimal benchmark instance with capabilities supplied by the test. */
  private static AbstractStsBenchmarkTest instance(boolean supportsAccessToken) {
    return new AbstractStsBenchmarkTest() {
      @Override
      protected Harness createHarness() {
        return null;
      }

      @Override
      protected String getProviderId() {
        return "test";
      }

      @Override
      protected boolean supportsGetAccessToken() {
        return supportsAccessToken;
      }
    };
  }

  private static AbstractStsBenchmarkTest.Harness harnessWithToken(String token) {
    return new AbstractStsBenchmarkTest.Harness() {
      @Override
      public StsClient createStsClient() {
        return null;
      }

      @Override
      public String getRoleName() {
        return "role";
      }

      @Override
      public String getWebIdentityToken() {
        return token;
      }

      @Override
      public void close() {}
    };
  }

  @Test
  void noAccessToken_noWebIdentityToken_excludesBoth() {
    List<String> excludes = instance(false).computeExcludes(harnessWithToken(null));
    assertTrue(excludes.contains(ACCESS_TOKEN), "no access-token capability drops getAccessToken");
    assertTrue(excludes.contains(WEB_IDENTITY), "no token must drop web-identity");
    assertEquals(2, excludes.size());
  }

  @Test
  void supportsAccessToken_noWebIdentityToken_excludesOnlyWebIdentity() {
    List<String> excludes = instance(true).computeExcludes(harnessWithToken(null));
    assertFalse(excludes.contains(ACCESS_TOKEN), "access-token capable; must not drop it");
    assertTrue(excludes.contains(WEB_IDENTITY), "no token must drop web-identity");
    assertEquals(1, excludes.size());
  }

  @Test
  void supportsAccessToken_withWebIdentityToken_excludesNothing() {
    List<String> excludes = instance(true).computeExcludes(harnessWithToken("oidc-jwt"));
    assertTrue(excludes.isEmpty(), "full-capability run must sweep every benchmark");
  }

  @Test
  void noAccessToken_withWebIdentityToken_excludesOnlyAccessToken() {
    List<String> excludes = instance(false).computeExcludes(harnessWithToken("oidc-jwt"));
    assertTrue(excludes.contains(ACCESS_TOKEN));
    assertFalse(excludes.contains(WEB_IDENTITY), "token present; web-identity must be swept");
    assertEquals(1, excludes.size());
  }

  @Test
  void blankWebIdentityToken_isTreatedAsAbsent() {
    List<String> excludes = instance(true).computeExcludes(harnessWithToken("   "));
    assertTrue(excludes.contains(WEB_IDENTITY), "blank token must drop web-identity");
  }
}
