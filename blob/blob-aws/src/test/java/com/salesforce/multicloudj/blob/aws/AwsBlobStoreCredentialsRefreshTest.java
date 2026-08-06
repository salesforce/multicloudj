package com.salesforce.multicloudj.blob.aws;

import com.salesforce.multicloudj.common.aws.ExpiredCredentialsInterceptor;
import com.salesforce.multicloudj.common.aws.RefreshingSessionCredentialsProvider;
import com.salesforce.multicloudj.common.retries.RetryConfig;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Verifies that the S3 client {@link AwsBlobStore} builds carries the pieces needed to keep working
 * past the lifetime of the session credentials it was built with.
 */
public class AwsBlobStoreCredentialsRefreshTest {

  @Test
  public void testSupplierBackedSessionCredentialsAttachTheExpiredCredentialsInterceptor() {
    AwsBlobStore.Builder builder = newBuilder(supplierBackedOverrider());

    try (AwsBlobStore store = builder.build()) {
      Assertions.assertInstanceOf(
          RefreshingSessionCredentialsProvider.class,
          builder.getS3Client().serviceClientConfiguration().credentialsProvider());
      Assertions.assertTrue(
          hasExpiredCredentialsInterceptor(builder.getS3Client()),
          "a refreshing credentials provider must come with the invalidating interceptor");
      Assertions.assertNotNull(store);
    }
  }

  @Test
  public void testValueBackedSessionCredentialsDoNotAttachTheExpiredCredentialsInterceptor() {
    CredentialsOverrider overrider =
        new CredentialsOverrider.Builder(CredentialsType.SESSION)
            .withSessionCredentials(new StsCredentials("key", "secret", "token"))
            .build();
    AwsBlobStore.Builder builder = newBuilder(overrider);

    try (AwsBlobStore store = builder.build()) {
      Assertions.assertFalse(hasExpiredCredentialsInterceptor(builder.getS3Client()));
      Assertions.assertNotNull(store);
    }
  }

  @Test
  public void testRetryConfigurationSurvivesAlongsideTheExpiredCredentialsInterceptor() {
    AwsBlobStore.Builder builder = newBuilder(supplierBackedOverrider());
    builder.withRetryConfig(
        RetryConfig.builder()
            .mode(RetryConfig.Mode.FIXED)
            .maxAttempts(4)
            .fixedDelayMillis(10L)
            .attemptTimeout(1_500L)
            .totalTimeout(9_000L)
            .build());

    try (AwsBlobStore store = builder.build()) {
      ClientOverrideConfiguration overrideConfiguration =
          builder.getS3Client().serviceClientConfiguration().overrideConfiguration();
      Assertions.assertTrue(hasExpiredCredentialsInterceptor(builder.getS3Client()));
      Assertions.assertEquals(
          Duration.ofMillis(1_500L), overrideConfiguration.apiCallAttemptTimeout().orElse(null));
      Assertions.assertEquals(
          Duration.ofMillis(9_000L), overrideConfiguration.apiCallTimeout().orElse(null));
      Assertions.assertTrue(overrideConfiguration.retryStrategy().isPresent());
      Assertions.assertNotNull(store);
    }
  }

  private static AwsBlobStore.Builder newBuilder(CredentialsOverrider overrider) {
    AwsBlobStore.Builder builder = new AwsBlobStore.Builder();
    builder.withCredentialsOverrider(overrider);
    builder.withBucket("bucket-1");
    builder.withRegion("us-east-2");
    return builder;
  }

  private static CredentialsOverrider supplierBackedOverrider() {
    return new CredentialsOverrider.Builder(CredentialsType.SESSION)
        .withSessionCredentialsSupplier(
            () ->
                new StsCredentials(
                    "key", "secret", "token", Instant.now().plus(Duration.ofHours(1))))
        .build();
  }

  private static boolean hasExpiredCredentialsInterceptor(S3Client s3Client) {
    List<ExecutionInterceptor> interceptors =
        s3Client.serviceClientConfiguration().overrideConfiguration().executionInterceptors();
    return interceptors.stream().anyMatch(i -> i instanceof ExpiredCredentialsInterceptor);
  }
}
