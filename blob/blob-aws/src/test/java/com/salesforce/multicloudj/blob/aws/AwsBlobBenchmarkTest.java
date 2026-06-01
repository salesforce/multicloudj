package com.salesforce.multicloudj.blob.aws;

import com.salesforce.multicloudj.blob.client.AbstractBlobBenchmarkTest;
import com.salesforce.multicloudj.blob.driver.AbstractBlobStore;
import java.net.URI;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AwsBlobBenchmarkTest extends AbstractBlobBenchmarkTest {

  private static final Logger logger = LoggerFactory.getLogger(AwsBlobBenchmarkTest.class);

  @Override
  protected String getProviderId() {
    return "aws";
  }

  @Override
  protected Harness createHarness() {
    return new HarnessImpl();
  }

  public static class HarnessImpl implements Harness {
    S3Client client;

    @Override
    public AbstractBlobStore createBlobStore() {
      String region = requireEnv("BLOB_BENCHMARK_AWS_REGION");
      String bucket = requireEnv("BLOB_BENCHMARK_AWS_BUCKET");
      String endpoint = "https://s3." + region + ".amazonaws.com";

      logger.info(
          "Creating AWS blob store with endpoint: {}, bucket: {}, region: {}",
          endpoint, bucket, region);

      try {
        URI endpointUri = URI.create(endpoint);

        client =
            S3Client.builder()
                .region(Region.of(region))
                .endpointOverride(endpointUri)
                .serviceConfiguration(
                    S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .build();

        logger.info("Successfully created S3 client");

        AwsBlobStore.Builder builder = new AwsBlobStore.Builder();
        builder
            .withS3Client(client)
            .withEndpoint(endpointUri)
            .withBucket(bucket)
            .withRegion(region);

        AbstractBlobStore blobStore = builder.build();
        logger.info("Successfully created AWS blob store");
        return blobStore;

      } catch (Exception e) {
        logger.error("Failed to create AWS blob store", e);

        if (client != null) {
          try {
            client.close();
          } catch (Exception cleanupException) {
            logger.warn("Failed to cleanup S3 client after failure", cleanupException);
          }
          client = null;
        }

        if (e instanceof RuntimeException) {
          throw (RuntimeException) e;
        } else {
          throw new RuntimeException("Failed to create AWS blob store", e);
        }
      }
    }

    @Override
    public String getBucketName() {
      return requireEnv("BLOB_BENCHMARK_AWS_BUCKET");
    }

    @Override
    public void close() throws Exception {
      if (client != null) {
        client.close();
      }
    }
  }
}
