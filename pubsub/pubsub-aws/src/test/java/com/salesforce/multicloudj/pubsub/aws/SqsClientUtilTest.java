package com.salesforce.multicloudj.pubsub.aws;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.retries.RetryConfig;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;

public class SqsClientUtilTest {

  private MockedStatic<SqsClient> sqsClientStatic;
  private SqsClientBuilder mockBuilder;

  @BeforeEach
  void setUp() {
    mockBuilder = mock(SqsClientBuilder.class);
    sqsClientStatic = mockStatic(SqsClient.class);
    sqsClientStatic.when(SqsClient::builder).thenReturn(mockBuilder);
    when(mockBuilder.region(any(Region.class))).thenReturn(mockBuilder);
    when(mockBuilder.overrideConfiguration(any(ClientOverrideConfiguration.class)))
        .thenReturn(mockBuilder);
    when(mockBuilder.build()).thenReturn(mock(SqsClient.class));
  }

  @AfterEach
  void tearDown() {
    sqsClientStatic.close();
  }

  @Test
  void testBuildSqsClient_WithRetryConfig_AppliesOverrideConfiguration() {
    RetryConfig retryConfig = RetryConfig.builder().maxAttempts(5).totalTimeout(3000L).build();

    SqsClientUtil.buildSqsClient("us-east-1", null, null, retryConfig);

    ArgumentCaptor<ClientOverrideConfiguration> captor =
        ArgumentCaptor.forClass(ClientOverrideConfiguration.class);
    verify(mockBuilder).overrideConfiguration(captor.capture());
    assertEquals(5, captor.getValue().retryStrategy().orElseThrow().maxAttempts());
    assertEquals(Duration.ofMillis(3000L), captor.getValue().apiCallTimeout().orElseThrow());
  }

  @Test
  void testBuildSqsClient_WithoutRetryConfig_KeepsSdkDefaults() {
    SqsClientUtil.buildSqsClient("us-east-1", null, null);

    verify(mockBuilder, never()).overrideConfiguration(any(ClientOverrideConfiguration.class));
  }

  @Test
  void testBuildSqsClient_WithInvalidRetryConfig_Throws() {
    RetryConfig retryConfig = RetryConfig.builder().maxAttempts(0).build();

    assertThrows(
        InvalidArgumentException.class,
        () -> SqsClientUtil.buildSqsClient("us-east-1", null, null, retryConfig));
    verify(mockBuilder, never()).build();
  }
}
