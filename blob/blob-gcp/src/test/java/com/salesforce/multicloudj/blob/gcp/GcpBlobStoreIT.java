package com.salesforce.multicloudj.blob.gcp;

import com.google.api.client.http.HttpTransport;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.HttpStorageOptions;
import com.google.cloud.storage.MultipartUploadClient;
import com.google.cloud.storage.MultipartUploadSettings;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.salesforce.multicloudj.blob.client.AbstractBlobStoreIT;
import com.salesforce.multicloudj.blob.driver.AbstractBlobStore;
import com.salesforce.multicloudj.blob.gcp.util.MultipartBoundaryTransformer;
import com.salesforce.multicloudj.common.gcp.util.MockGoogleCredentialsFactory;
import com.salesforce.multicloudj.common.gcp.util.TestsUtilGcp;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class GcpBlobStoreIT extends AbstractBlobStoreIT {

  private static final String endpoint = "https://storage.googleapis.com";
  private static final String bucketName = "substrate-sdk-gcp-poc1-test-bucket";
  private static final String versionedBucketName = "substrate-sdk-gcp-poc1-test-bucket-versioned";
  private static final String nonExistentBucketName = "java-bucket-does-not-exist";

  @Override
  protected Harness createHarness() {
    return new HarnessImpl();
  }

  public static class HarnessImpl implements Harness {
    int port = ThreadLocalRandom.current().nextInt(2000, 20000);

    Storage storage;

    private static class MockGoogleCredentialsWrapper extends GoogleCredentials
        implements com.google.auth.ServiceAccountSigner {
      private final GoogleCredentials delegate;

      MockGoogleCredentialsWrapper(GoogleCredentials delegate) {
        this.delegate = delegate;
      }

      @Override
      public com.google.auth.oauth2.AccessToken refreshAccessToken() throws IOException {
        return delegate.refreshAccessToken();
      }

      @Override
      public String getAccount() {
        return "mock-service-account@mock-project.iam.gserviceaccount.com";
      }

      @Override
      public byte[] sign(byte[] toSign) {
        return "mock-signature".getBytes(java.nio.charset.StandardCharsets.UTF_8);
      }
    }

    @Override
    public AbstractBlobStore createBlobStore(
        boolean useValidBucket, boolean useValidCredentials, boolean useVersionedBucket) {

      String bucketNameToUse =
          useValidBucket
              ? (useVersionedBucket ? versionedBucketName : bucketName)
              : nonExistentBucketName;

      boolean isRecordingEnabled = System.getProperty("record") != null;

      if (isRecordingEnabled && useValidCredentials) {
        // Live recording path – rely on real ADC
        try {
          Credentials credentials = GoogleCredentials.getApplicationDefault();
          
          // If the credentials don't implement ServiceAccountSigner (e.g. UserCredentials),
          // we wrap them in a mock signer just so the test can generate the URL.
          // Note: The generated URL won't be valid for actual use unless we use IAM credentials
          // API, but this allows the test to proceed and wiremock to record the request/response.
          if (!(credentials instanceof com.google.auth.ServiceAccountSigner)) {
            credentials = new MockGoogleCredentialsWrapper((GoogleCredentials) credentials);
          }
          
          return createBlobStore(bucketNameToUse, credentials);
        } catch (IOException e) {
          // Fallback to NoCredentials if unable to load application default credentials
          return createBlobStore(bucketNameToUse, NoCredentials.getInstance());
        }
      } else {
        // Replay path - inject mock credentials
        GoogleCredentials mockCreds = MockGoogleCredentialsFactory.createMockCredentials();
        return createBlobStore(bucketNameToUse, mockCreds);
      }
    }

    private AbstractBlobStore createBlobStore(
        final String bucketName, final Credentials credentials) {

      HttpTransport httpTransport = TestsUtilGcp.getHttpTransport(port);
      HttpTransportOptions transportOptions =
          HttpTransportOptions.newBuilder().setHttpTransportFactory(() -> httpTransport).build();

      Storage storage =
          StorageOptions.newBuilder()
              .setTransportOptions(transportOptions)
              .setCredentials(credentials)
              .setHost(endpoint)
              .build()
              .getService();

      HttpStorageOptions.Builder storageOptionsBuilder =
          HttpStorageOptions.http().setTransportOptions(transportOptions);
      MultipartUploadClient mpuClient =
          MultipartUploadClient.create(MultipartUploadSettings.of(storageOptionsBuilder.build()));
      return new GcpBlobStore.Builder()
          .withStorage(storage)
          .withMultipartUploadClient(mpuClient)
          .withEndpoint(URI.create(endpoint))
          .withBucket(bucketName)
          .build();
    }

    @Override
    public String getEndpoint() {
      return endpoint;
    }

    @Override
    public String getProviderId() {
      return "gcp";
    }

    @Override
    public String getMetadataHeader(String key) {
      return "x-goog-meta-" + key;
    }

    @Override
    public String getTaggingHeader() {
      return ""; // Tagging headers don't exist in GCP
    }

    @Override
    public int getPort() {
      return port;
    }

    @Override
    public String getKmsKeyId() {
      return "projects/substrate-sdk-gcp-poc1/locations/us"
          + "/keyRings/chameleon-test/cryptoKeys/chameleon-test";
    }

    @Override
    public boolean isSha256Supported() {
      return false;
    }

    @Override
    public List<String> getWiremockExtensions() {
      return List.of(MultipartBoundaryTransformer.class.getName());
    }

    @Override
    public void close() {
      try {
        storage.close();
      } catch (Exception e) {
        // Burying the exception
      }
    }
  }
}
