package com.salesforce.multicloudj.pubsub.aws;

import com.salesforce.multicloudj.common.aws.CredentialsProvider;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import java.net.URI;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;

/** Utility class for building SQS clients with common configuration. */
public final class SqsClientUtil {

  private SqsClientUtil() {}

  public static SqsClient buildSqsClient(
      String region, URI endpoint, CredentialsOverrider credentialsOverrider) {
    SqsClientBuilder clientBuilder = SqsClient.builder();

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

    return clientBuilder.build();
  }
}
