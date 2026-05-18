package com.salesforce.multicloudj.blob.gcp;

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.auth.Credentials;
import com.google.auth.ServiceAccountSigner;
import com.google.auth.oauth2.AccessToken;
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
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class GcpBlobStoreIT extends AbstractBlobStoreIT {

  private static final String endpoint = "https://storage.googleapis.com";
  private static final String bucketName = "substrate-sdk-gcp-poc1-test-bucket";
  private static final String versionedBucketName = "substrate-sdk-gcp-poc1-test-bucket-versioned";
  private static final String nonExistentBucketName = "java-bucket-does-not-exist";
  // Dedicated bucket name for testInvalidCredentials. Using a unique name ensures its
  // WireMock scenario names don't collide with other tests that share the main bucket URL.
  private static final String invalidCredsBucketName = "invalid-credentials-test-bucket";

  @Override
  protected Harness createHarness() {
    return new HarnessImpl();
  }

  public static class HarnessImpl implements Harness {
    int port = ThreadLocalRandom.current().nextInt(2000, 20000);

    Storage storage;

    private static class MockGoogleCredentialsWrapper extends GoogleCredentials
        implements ServiceAccountSigner {
      private final GoogleCredentials delegate;

      MockGoogleCredentialsWrapper(GoogleCredentials delegate) {
        this.delegate = delegate;
      }

      @Override
      public AccessToken refreshAccessToken() throws IOException {
        return delegate.refreshAccessToken();
      }

      @Override
      public String getAccount() {
        return "mock-service-account@mock-project.iam.gserviceaccount.com";
      }

      @Override
      public byte[] sign(byte[] toSign) {
        return "mock-signature".getBytes(StandardCharsets.UTF_8);
      }
    }

    @Override
    public AbstractBlobStore createBlobStore(
        boolean useValidBucket, boolean useValidCredentials, boolean useVersionedBucket) {

      // When credentials are intentionally invalid, use a dedicated bucket name so that
      // WireMock scenario names (which embed the bucket path) are unique to this test
      // and cannot collide with stubs from other tests that share the main bucket.
      String bucketNameToUse =
          !useValidCredentials
              ? invalidCredsBucketName
              : useValidBucket
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
          if (!(credentials instanceof ServiceAccountSigner)) {
            credentials = new MockGoogleCredentialsWrapper((GoogleCredentials) credentials);
          }

          return createBlobStore(bucketNameToUse, credentials, false);
        } catch (IOException e) {
          // Fallback to NoCredentials if unable to load application default credentials
          return createBlobStore(bucketNameToUse, NoCredentials.getInstance(), false);
        }
      } else {
        // Replay path - inject mock credentials. When credentials are intentionally invalid,
        // disable HTTP-level retries so each operation produces exactly one WireMock recording
        // instead of 11 (1 original + 10 default retries on 401).
        GoogleCredentials mockCreds = MockGoogleCredentialsFactory.createMockCredentials();
        return createBlobStore(bucketNameToUse, mockCreds, !useValidCredentials);
      }
    }

    private AbstractBlobStore createBlobStore(
        final String bucketName, final Credentials credentials, final boolean disableRetries) {

      HttpTransport httpTransport = TestsUtilGcp.getHttpTransport(port);
      HttpTransportOptions transportOptions =
          HttpTransportOptions.newBuilder().setHttpTransportFactory(() -> httpTransport).build();

      StorageOptions.Builder storageBuilder =
          StorageOptions.newBuilder()
              .setTransportOptions(transportOptions)
              .setCredentials(credentials)
              .setHost(endpoint);

      if (disableRetries) {
        // Wrap the transport options to set numRetries=0 on every HTTP request, preventing
        // the google-http-client from retrying 401 responses due to credential refresh attempts.
        HttpTransportOptions noRetryTransportOptions =
            new HttpTransportOptions(
                HttpTransportOptions.newBuilder()
                    .setHttpTransportFactory(() -> httpTransport)) {
              @Override
              public HttpRequestInitializer getHttpRequestInitializer(
                  com.google.cloud.ServiceOptions<?, ?> serviceOptions) {
                HttpRequestInitializer delegate =
                    super.getHttpRequestInitializer(serviceOptions);
                return request -> {
                  if (delegate != null) {
                    delegate.initialize(request);
                  }
                  request.setNumberOfRetries(0);
                };
              }
            };
        storageBuilder.setTransportOptions(noRetryTransportOptions);
      }

      Storage storage = storageBuilder.build().getService();

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
    public boolean isObjectLockSupported() {
      return true;
    }

    @Override
    public boolean isListObjectVersionsSupported() {
      return true;
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
