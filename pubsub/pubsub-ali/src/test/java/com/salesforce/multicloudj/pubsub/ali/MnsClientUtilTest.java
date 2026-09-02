package com.salesforce.multicloudj.pubsub.ali;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.aliyun.mns.client.CloudAccount;
import com.aliyun.mns.client.MNSClient;
import com.aliyun.mns.common.http.ClientConfiguration;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.net.URI;
import org.junit.jupiter.api.Test;

public class MnsClientUtilTest {

  private static final URI ENDPOINT =
      URI.create("https://1234567890.mns.cn-shanghai.aliyuncs.com");

  private static CredentialsOverrider sessionOverrider() {
    return new CredentialsOverrider.Builder(CredentialsType.SESSION)
        .withSessionCredentials(new StsCredentials("key", "secret", "token"))
        .build();
  }

  @Test
  void nullEndpointThrows() {
    assertThrows(
        InvalidArgumentException.class,
        () -> MnsClientUtil.buildCloudAccount(null, sessionOverrider(), null));
  }

  @Test
  void malformedEndpointsThrow() {
    // relative / hostless
    assertThrows(
        InvalidArgumentException.class,
        () -> MnsClientUtil.buildCloudAccount(
            URI.create("not-an-absolute-endpoint"), sessionOverrider(), null));
    // unsupported scheme
    assertThrows(
        InvalidArgumentException.class,
        () -> MnsClientUtil.buildCloudAccount(
            URI.create("ftp://account.mns.example.com"), sessionOverrider(), null));
    // path-bearing
    assertThrows(
        InvalidArgumentException.class,
        () -> MnsClientUtil.buildCloudAccount(
            URI.create("https://account.mns.example.com/unexpected/path"), sessionOverrider(),
            null));
  }

  @Test
  void nullCredentialsThrows() {
    assertThrows(
        InvalidArgumentException.class,
        () -> MnsClientUtil.buildCloudAccount(ENDPOINT, null, null));
  }

  @Test
  void buildCloudAccountUsesTheGivenEndpoint() {
    CloudAccount cloudAccount = MnsClientUtil.buildCloudAccount(ENDPOINT, sessionOverrider(), null);
    assertEquals(ENDPOINT.toString(), cloudAccount.getAccountEndpoint());
  }

  @Test
  void malformedProxiesThrow() {
    // hostless (URI parses "localhost" as scheme, "8888" as scheme-specific part -> null host)
    assertThrows(
        InvalidArgumentException.class,
        () -> MnsClientUtil.buildClientConfiguration(URI.create("localhost:8888")));
    // unsupported scheme
    assertThrows(
        InvalidArgumentException.class,
        () -> MnsClientUtil.buildClientConfiguration(URI.create("ftp://proxy.example.com:3128")));
  }

  @Test
  void buildClientConfigurationAppliesProxy() {
    ClientConfiguration config =
        MnsClientUtil.buildClientConfiguration(URI.create("http://localhost:8888"));
    assertEquals("localhost", config.getProxyHost());
    assertEquals(8888, config.getProxyPort());
  }

  @Test
  void buildClientConfigurationWithoutProxyLeavesHostUnset() {
    ClientConfiguration config = MnsClientUtil.buildClientConfiguration(null);
    assertNull(config.getProxyHost());
  }

  @Test
  void buildMnsClientReturnsClientForValidInputs() {
    MNSClient client = MnsClientUtil.buildMnsClient(ENDPOINT, sessionOverrider(), null);
    try {
      assertNotNull(client);
    } finally {
      client.close();
    }
  }
}
