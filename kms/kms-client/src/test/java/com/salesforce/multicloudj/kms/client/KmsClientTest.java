package com.salesforce.multicloudj.kms.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.common.retries.RetryConfig;
import com.salesforce.multicloudj.kms.model.CreateKeyRequest;
import com.salesforce.multicloudj.kms.model.KeyRef;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class KmsClientTest {

  private static final KeyRef KEY = KeyRef.builder().keyId("key-1").build();

  @Test
  void builderPassesConfigurationToProvider() {
    Properties properties = new Properties();
    properties.setProperty("name", "value");
    RetryConfig retryConfig =
        RetryConfig.builder().mode(RetryConfig.Mode.FIXED).fixedDelayMillis(10).build();

    KmsClient client =
        KmsClient.builder(TestKms.PROVIDER_ID)
            .withRegion("region-1")
            .withEndpoint(URI.create("https://kms.example.com"))
            .withKeyContainer("container-1")
            .withRetryConfig(retryConfig)
            .withProperties(properties)
            .build();

    TestKms kms = (TestKms) client.kms;
    assertEquals(TestKms.PROVIDER_ID, kms.getProviderId());
    assertEquals("region-1", kms.getRegion());
    assertEquals("container-1", kms.getKeyContainer());
    assertSame(retryConfig, kms.configuredRetryConfig());
    assertSame(properties, kms.configuredProperties());
  }

  @Test
  void builderRejectsUnknownProvider() {
    assertThrows(IllegalArgumentException.class, () -> KmsClient.builder("no-such-provider"));
  }

  @Test
  void builderRejectsInvalidRetryConfig() {
    RetryConfig invalid = RetryConfig.builder().mode(RetryConfig.Mode.FIXED).build();

    assertThrows(
        InvalidArgumentException.class,
        () -> KmsClient.builder(TestKms.PROVIDER_ID).withRetryConfig(invalid));
  }

  @Test
  void providerFailuresAreTranslatedByMapException() {
    TestKms kms = new TestKms();
    kms.failure = new IllegalStateException("native failure");
    KmsClient client = new KmsClient(kms);

    SubstrateSdkException thrown =
        assertThrows(SubstrateSdkException.class, () -> client.enableKey(KEY));

    assertInstanceOf(UnknownException.class, thrown);
    assertSame(kms.failure, thrown.getCause());
  }

  @Test
  void substrateExceptionsPassThroughUnchanged() {
    TestKms kms = new TestKms();
    ResourceNotFoundException notFound = new ResourceNotFoundException("missing");
    kms.failure = notFound;
    KmsClient client = new KmsClient(kms);

    assertSame(
        notFound, assertThrows(ResourceNotFoundException.class, () -> client.disableKey(KEY)));
  }

  @Test
  void operationsDelegateToProvider() throws Exception {
    TestKms kms = new TestKms();
    KmsClient client = new KmsClient(kms);

    assertEquals(
        "key-1", client.createKey(CreateKeyRequest.builder().build()).getKeyRef().getKeyId());
    client.enableKey(KEY);
    client.disableKey(KEY);
    client.tagKey(KEY, Map.of("team", "orders"));
    client.getKeyPolicy(KEY);
    client.close();

    assertEquals(
        List.of("createKey", "enableKey", "disableKey", "tagKey", "getKeyPolicy", "close"),
        kms.calls);
  }
}
