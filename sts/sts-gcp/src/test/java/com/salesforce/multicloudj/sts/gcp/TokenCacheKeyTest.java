package com.salesforce.multicloudj.sts.gcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Verifies the equality contract of {@link TokenCacheKey}: keys are equal iff every qualifying
 * input matches, are never equal across paths, and treat a {@code null} input the same as an empty
 * string so a cached entry is only ever returned for an identical request.
 */
class TokenCacheKeyTest {

  @Test
  void sameInputs_areEqualAndShareHashCode() {
    TokenCacheKey a = TokenCacheKey.forAssumeRole("role", 3600L, "scopeHash");
    TokenCacheKey b = TokenCacheKey.forAssumeRole("role", 3600L, "scopeHash");

    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
  }

  @Test
  void sameKey_isEqualToItself() {
    TokenCacheKey key = TokenCacheKey.forAccessToken("scope");
    assertEquals(key, key);
  }

  @Test
  void differentType_isNotEqual() {
    TokenCacheKey key = TokenCacheKey.forAccessToken("scope");
    assertNotEquals(key, "scope");
    assertNotEquals(key, null);
  }

  @Test
  void keysAreNeverEqualAcrossPaths() {
    // Same primary value, different path: must not collide.
    TokenCacheKey accessToken = TokenCacheKey.forAccessToken("value");
    TokenCacheKey callerIdentity = TokenCacheKey.forCallerIdentity("value");

    assertNotEquals(accessToken, callerIdentity);
  }

  @Test
  void assumeRole_distinguishesEveryInput() {
    TokenCacheKey base = TokenCacheKey.forAssumeRole("role", 3600L, "scopeHash");

    assertNotEquals(base, TokenCacheKey.forAssumeRole("other-role", 3600L, "scopeHash"));
    assertNotEquals(base, TokenCacheKey.forAssumeRole("role", 7200L, "scopeHash"));
    assertNotEquals(base, TokenCacheKey.forAssumeRole("role", 3600L, "other-scopeHash"));
  }

  @Test
  void webIdentity_distinguishesAudienceAndTokenHash() {
    TokenCacheKey base = TokenCacheKey.forWebIdentity("aud", "tokenHash");

    assertNotEquals(base, TokenCacheKey.forWebIdentity("other-aud", "tokenHash"));
    assertNotEquals(base, TokenCacheKey.forWebIdentity("aud", "other-tokenHash"));
  }

  @Test
  void nullInputsAreTreatedAsEmpty() {
    // A null and an empty string for the same field produce the same key.
    assertEquals(TokenCacheKey.forAccessToken(null), TokenCacheKey.forAccessToken(""));
    assertEquals(TokenCacheKey.forCallerIdentity(null), TokenCacheKey.forCallerIdentity(""));
    assertEquals(
        TokenCacheKey.forAssumeRole(null, 0L, null), TokenCacheKey.forAssumeRole("", 0L, ""));
    assertEquals(
        TokenCacheKey.forWebIdentity(null, null), TokenCacheKey.forWebIdentity("", ""));
  }

  @Test
  void toString_rendersPathAndInputs() {
    String rendered = TokenCacheKey.forAssumeRole("role", 3600L, "scopeHash").toString();

    assertTrue(rendered.contains("ASSUME_ROLE"));
    assertTrue(rendered.contains("role"));
    assertTrue(rendered.contains("scopeHash"));
    assertTrue(rendered.contains("3600"));
  }
}
