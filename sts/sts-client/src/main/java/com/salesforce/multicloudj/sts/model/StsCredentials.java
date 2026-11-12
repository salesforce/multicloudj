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
   * Returns the time at which the provider reports these credentials expire, or {@code null} when
   * the provider did not report an expiry.
   */
  public Instant getExpiration() {
    return expiration;
  }
}
