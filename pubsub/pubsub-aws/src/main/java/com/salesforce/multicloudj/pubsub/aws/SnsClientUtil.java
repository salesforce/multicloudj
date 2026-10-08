package com.salesforce.multicloudj.pubsub.aws;

import com.salesforce.multicloudj.common.aws.CredentialsProvider;
import com.salesforce.multicloudj.common.retries.RetryConfig;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import java.net.URI;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.SnsClientBuilder;

/** Utility class for building SNS clients with common configuration. */
public final class SnsClientUtil {

  private SnsClientUtil() {}

  public static SnsClient buildSnsClient(
      String region, URI endpoint, CredentialsOverrider credentialsOverrider) {
    return buildSnsClient(region, endpoint, credentialsOverrider, null);
  }

  public static SnsClient buildSnsClient(
      String region,
      URI endpoint,
      CredentialsOverrider credentialsOverrider,
      RetryConfig retryConfig) {
    SnsClientBuilder clientBuilder = SnsClient.builder();

    // Set region if provided
    if (region != null) {
      clientBuilder.region(Region.of(region));
    }

    // Set endpoint if provided
    if (endpoint != null) {
      clientBuilder.endpointOverride(endpoint);
    }

    // Set credentials if provided
    if (credentialsOverrider != null) {
      AwsCredentialsProvider credentialsProvider =
          CredentialsProvider.getCredentialsProvider(
              credentialsOverrider, region != null ? Region.of(region) : null);
      if (credentialsProvider != null) {
        clientBuilder.credentialsProvider(credentialsProvider);
      }
    }

    if (retryConfig != null) {
      clientBuilder.overrideConfiguration(
          RetryConfigUtil.toClientOverrideConfiguration(retryConfig));
    }

    return clientBuilder.build();
  }
}
