package com.salesforce.multicloudj.sts.model;

import java.time.Instant;

public class StsCredentials {
  String accessKeyId;
  String accessKeySecret;
  String securityToken;
  Instant expiration;

  public StsCredentials(String accessKeyId, String accessKeySecret, String securityToken) {
    this(accessKeyId, accessKeySecret, securityToken, null);
  }

  public StsCredentials(
      String accessKeyId, String accessKeySecret, String securityToken, Instant expiration) {
    this.accessKeyId = accessKeyId;
    this.accessKeySecret = accessKeySecret;
    this.securityToken = securityToken;
    this.expiration = expiration;
  }

  public String getAccessKeyId() {
    return accessKeyId;
  }

  public String getSecurityToken() {
    return securityToken;
  }

  public String getAccessKeySecret() {
    return accessKeySecret;
  }

  /**
   * The instant at which these credentials stop being valid, or {@code null} if the caller did not
   * declare one.
   *
   * <p>This is only consulted for credentials supplied through {@link
   * CredentialsOverrider.Builder#withSessionCredentialsSupplier}, where it schedules the renewal
   * that invokes the supplier again; declaring it lets renewal happen before the credentials
   * lapse, and omitting it leaves renewal to a fixed interval that cannot make that guarantee.
   * Credentials supplied by value through {@link
   * CredentialsOverrider.Builder#withSessionCredentials} cannot be renewed at all, so the
   * expiration is ignored on that path and the credentials are used until the service rejects
   * them.
   *
   * @return the expiration instant, or {@code null} if unknown
   */
  public Instant getExpiration() {
    return expiration;
  }
}
