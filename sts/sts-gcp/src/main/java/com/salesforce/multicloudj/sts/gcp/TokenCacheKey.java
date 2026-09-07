package com.salesforce.multicloudj.sts.gcp;

import lombok.Value;

/**
 * Immutable cache key for {@link GcpStsTokenCache}. Each of the GCP STS token paths keys on the
 * bounded set of inputs that fully qualify the token it produces, so a cached entry is never
 * returned for a request that would have produced a different token (different identity, scope,
 * access boundary, audience, or subject token).
 *
 * <p>Secret-bearing inputs (the access boundary, the web-identity subject token) are represented in
 * the key by a hash supplied by the caller — never the raw value. {@code toString} renders only
 * {@code path}/{@code primary}/{@code secondary}/{@code numeric}, which are non-secret identifiers,
 * hashes, or lifetimes; it never exposes a raw secret.
 */
@Value
public class TokenCacheKey {

  /** The GCP STS path a key belongs to; part of equality so keys never collide across paths. */
  public enum Path {
    ASSUME_ROLE,
    ACCESS_TOKEN,
    CALLER_IDENTITY,
    WEB_IDENTITY
  }

  /** The GCP STS path this key belongs to; part of equality so keys never collide. */
  Path path;

  /** The primary identity/scope/audience discriminator for the path. */
  String primary;

  /** The secondary discriminator (a hash for secret-bearing inputs), or empty. */
  String secondary;

  /** The numeric discriminator (e.g. requested lifetime in seconds), or 0 when unset. */
  long numeric;

  /**
   * Key for the impersonation / downscoping path.
   *
   * @param role target principal (empty when no impersonation)
   * @param expiration requested lifetime in seconds (0 when unset)
   * @param credentialScopeHash hash of the serialized access boundary (empty when no downscoping)
   */
  public static TokenCacheKey forAssumeRole(
      String role, long expiration, String credentialScopeHash) {
    return new TokenCacheKey(Path.ASSUME_ROLE, nullToEmpty(role), nullToEmpty(credentialScopeHash),
        expiration);
  }

  /**
   * Key for the plain access-token path.
   *
   * @param scope the OAuth scope the token is minted for
   */
  public static TokenCacheKey forAccessToken(String scope) {
    return new TokenCacheKey(Path.ACCESS_TOKEN, nullToEmpty(scope), "", 0L);
  }

  /**
   * Key for the caller-identity (id-token) path.
   *
   * @param aud the target audience the id token is minted for
   */
  public static TokenCacheKey forCallerIdentity(String aud) {
    return new TokenCacheKey(Path.CALLER_IDENTITY, nullToEmpty(aud), "", 0L);
  }

  /**
   * Key for the web-identity token-exchange path.
   *
   * @param audience the identity-pool provider audience
   * @param webIdentityTokenHash hash of the subject token being exchanged
   */
  public static TokenCacheKey forWebIdentity(String audience, String webIdentityTokenHash) {
    return new TokenCacheKey(Path.WEB_IDENTITY, nullToEmpty(audience),
        nullToEmpty(webIdentityTokenHash), 0L);
  }

  private static String nullToEmpty(String value) {
    return value == null ? "" : value;
  }
}
