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
   * Returns when these credentials expire, or {@code null} if unknown (not reported or
   * unparseable). {@code null} does not mean the credentials never expire.
   */
  public Instant getExpiration() {
    return expiration;
  }
}
