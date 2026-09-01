package com.salesforce.multicloudj.pubsub.ali;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.aliyun.mns.client.MNSClient;
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
        () -> MnsClientUtil.buildMnsClient(null, sessionOverrider(), null));
  }

  @Test
  void buildsClientWithSessionCredentials() {
    MNSClient client = MnsClientUtil.buildMnsClient(ENDPOINT, sessionOverrider(), null);
    try {
      assertNotNull(client);
    } finally {
      client.close();
    }
  }

  @Test
  void buildsClientWithProxy() {
    MNSClient client =
        MnsClientUtil.buildMnsClient(ENDPOINT, sessionOverrider(), URI.create("http://localhost:8888"));
    try {
      assertNotNull(client);
    } finally {
      client.close();
    }
  }

  @Test
  void nullCredentialsThrows() {
    assertThrows(
        InvalidArgumentException.class, () -> MnsClientUtil.buildMnsClient(ENDPOINT, null, null));
  }
}
