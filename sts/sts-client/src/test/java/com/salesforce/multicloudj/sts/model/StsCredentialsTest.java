package com.salesforce.multicloudj.sts.model;

import java.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class StsCredentialsTest {

  @Test
  void stsCredentials() {
    String accessKeyId = "testKeyId";
    String secretAccessKey = "testSecretKey";
    String token = "testToken";

    StsCredentials credentials = new StsCredentials(accessKeyId, secretAccessKey, token);
    Assertions.assertEquals(
        accessKeyId, credentials.getAccessKeyId(), "AccessKey Id doesn't match");
    Assertions.assertEquals(
        secretAccessKey, credentials.getAccessKeySecret(), "Secret Key doesn't match");
    Assertions.assertEquals(token, credentials.getSecurityToken(), "Token doesn't match");
    Assertions.assertNull(credentials.getExpiration());
  }

  @Test
  void stsCredentialsWithExpiration() {
    Instant expiration = Instant.parse("2030-01-01T00:00:00Z");

    StsCredentials credentials = new StsCredentials("id", "secret", "token", expiration);
    Assertions.assertEquals(expiration, credentials.getExpiration());
  }
}
