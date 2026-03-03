package com.salesforce.multicloudj.blob.gcp;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.salesforce.multicloudj.blob.driver.ListBucketsResponse;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import com.salesforce.multicloudj.common.retries.RetryConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GcpBlobClientTest {

    private Storage mockStorage;
    private GcpBlobClient gcpBlobClient;

    @BeforeEach
    void setup() {
        mockStorage = mock(Storage.class);
        StsCredentials sessionCreds = new StsCredentials("key-1", "secret-1", "token-1");
        CredentialsOverrider credsOverrider = new CredentialsOverrider.Builder(CredentialsType.SESSION)
                .withSessionCredentials(sessionCreds).build();

        GcpBlobClient.Builder builder = new GcpBlobClient.Builder();
        builder.withCredentialsOverrider(credsOverrider)
                .withRegion("us-west")
                .withEndpoint(URI.create("https://storage.googleapis.com"))
                .withProxyEndpoint(URI.create("https://proxy.endpoint.com:443"));

        gcpBlobClient = builder.build(mockStorage);
    }

    @Test
    void testListBuckets() {
        // Create mock Bucket objects
        Bucket bucket1 = mock(Bucket.class);
        when(bucket1.getName()).thenReturn("bucket1");
        when(bucket1.getLocation()).thenReturn("us-west1");
        when(bucket1.getCreateTimeOffsetDateTime()).thenReturn(OffsetDateTime.now());

        Bucket bucket2 = mock(Bucket.class);
        when(bucket2.getName()).thenReturn("bucket2");
        when(bucket2.getLocation()).thenReturn("us-east1");
        when(bucket2.getCreateTimeOffsetDateTime()).thenReturn(OffsetDateTime.now());

        com.google.api.gax.paging.Page<Bucket> mockPage = mock(com.google.api.gax.paging.Page.class);
        when(mockPage.iterateAll()).thenReturn(Arrays.asList(bucket1, bucket2));
        when(mockStorage.list()).thenReturn(mockPage);

        // Call the method to be tested
        ListBucketsResponse response = gcpBlobClient.listBuckets();

        // Verify that list was called on the mock Storage client
        verify(mockStorage).list();

        // Assert that the result matches the expected buckets
        assertNotNull(response);
        assertEquals(2, response.getBucketInfoList().size());
        assertEquals("bucket1", response.getBucketInfoList().get(0).getName());
        assertEquals("bucket2", response.getBucketInfoList().get(1).getName());
    }

    @Test
    void testCreateBucket() {
        String bucketName = "test-bucket";
        Bucket mockBucket = mock(Bucket.class);
        when(mockStorage.create(any(com.google.cloud.storage.BucketInfo.class))).thenReturn(mockBucket);

        // Call createBucket
        gcpBlobClient.createBucket(bucketName);

        // Verify that create was called on the mock Storage client
        verify(mockStorage).create(any(com.google.cloud.storage.BucketInfo.class));
    }

    @Test
    void testProviderId() {
        assertEquals("gcp", gcpBlobClient.getProviderId());
    }

    @Test
    void testExceptionHandlingStorageException() {
        // Test 404 error
        StorageException notFoundException = new StorageException(404, "Not found");
        Class<?> cls = gcpBlobClient.getException(notFoundException);
        assertEquals(ResourceNotFoundException.class, cls);

        // Test 400 error
        StorageException badRequestException = new StorageException(400, "Bad request");
        cls = gcpBlobClient.getException(badRequestException);
        assertEquals(InvalidArgumentException.class, cls);
    }

    @Test
    void testExceptionHandlingApiException() {
        // Test NOT_FOUND
        ApiException notFoundException = mock(ApiException.class);
        StatusCode notFoundStatusCode = mock(StatusCode.class);
        when(notFoundStatusCode.getCode()).thenReturn(StatusCode.Code.NOT_FOUND);
        when(notFoundException.getStatusCode()).thenReturn(notFoundStatusCode);

        Class<?> cls = gcpBlobClient.getException(notFoundException);
        assertEquals(ResourceNotFoundException.class, cls);

        // Test INVALID_ARGUMENT
        ApiException invalidArgException = mock(ApiException.class);
        StatusCode invalidArgStatusCode = mock(StatusCode.class);
        when(invalidArgStatusCode.getCode()).thenReturn(StatusCode.Code.INVALID_ARGUMENT);
        when(invalidArgException.getStatusCode()).thenReturn(invalidArgStatusCode);

        cls = gcpBlobClient.getException(invalidArgException);
        assertEquals(InvalidArgumentException.class, cls);
    }

    @Test
    void testExceptionHandlingIllegalArgument() {
        IllegalArgumentException illegalArgException = new IllegalArgumentException("Invalid argument");
        Class<?> cls = gcpBlobClient.getException(illegalArgException);
        assertEquals(InvalidArgumentException.class, cls);
    }

    @Test
    void testExceptionHandlingUnknown() {
        RuntimeException unknownException = new RuntimeException("Unknown error");
        Class<?> cls = gcpBlobClient.getException(unknownException);
        assertEquals(UnknownException.class, cls);
    }

    @Test
    void testBuildStorageWithDefaultConfiguration() {
        // Build with minimal configuration
        GcpBlobClient.Builder builder = new GcpBlobClient.Builder();
        GcpBlobClient client = builder.build();

        // Verify the client was created successfully
        assertNotNull(client);
        assertEquals("gcp", client.getProviderId());
    }

    @Test
    void testBuildStorageWithCredentialsOverrider() {
        // Build with credentials overrider
        StsCredentials sessionCreds = new StsCredentials("access-key", "secret-key", "session-token");
        CredentialsOverrider credsOverrider = new CredentialsOverrider.Builder(CredentialsType.SESSION)
                .withSessionCredentials(sessionCreds).build();

        GcpBlobClient.Builder builder = new GcpBlobClient.Builder();
        builder.withCredentialsOverrider(credsOverrider);
        GcpBlobClient client = builder.build();

        // Verify the client was created successfully
        assertNotNull(client);
        assertEquals("gcp", client.getProviderId());
    }

    @Test
    void testBuildStorageWithCustomEndpoint() {
        // Build with custom endpoint
        GcpBlobClient.Builder builder = new GcpBlobClient.Builder();
        builder.withEndpoint(URI.create("https://custom-endpoint.googleapis.com"));
        GcpBlobClient client = builder.build();

        // Verify the client was created successfully
        assertNotNull(client);
        assertEquals("gcp", client.getProviderId());
    }

    @Test
    void testBuildStorageWithProxyEndpoint() {
        // Build with proxy endpoint
        GcpBlobClient.Builder builder = new GcpBlobClient.Builder();
        builder.withProxyEndpoint(URI.create("https://proxy.example.com:8080"));
        GcpBlobClient client = builder.build();

        // Verify the client was created successfully
        assertNotNull(client);
        assertEquals("gcp", client.getProviderId());
    }

    @Test
    void testBuildStorageWithAllConfigurations() {
        // Build with all configurations
        StsCredentials sessionCreds = new StsCredentials("access-key", "secret-key", "session-token");
        CredentialsOverrider credsOverrider = new CredentialsOverrider.Builder(CredentialsType.SESSION)
                .withSessionCredentials(sessionCreds).build();

        GcpBlobClient.Builder builder = new GcpBlobClient.Builder();
        builder.withCredentialsOverrider(credsOverrider)
                .withEndpoint(URI.create("https://custom-endpoint.googleapis.com"))
                .withProxyEndpoint(URI.create("https://proxy.example.com:8080"))
                .withRegion("us-central1");
        GcpBlobClient client = builder.build();

        // Verify the client was created successfully
        assertNotNull(client);
        assertEquals("gcp", client.getProviderId());
    }

    @Test
    void testBuildStorageWithRetryConfig() {
        // Build with retry configuration
        RetryConfig retryConfig = RetryConfig.builder()
                .mode(RetryConfig.Mode.EXPONENTIAL)
                .maxAttempts(3)
                .initialDelayMillis(100L)
                .maxDelayMillis(1000L)
                .multiplier(2.0)
                .build();

        GcpBlobClient.Builder builder = new GcpBlobClient.Builder();
        builder.withRetryConfig(retryConfig);
        GcpBlobClient client = builder.build();

        // Verify the client was created successfully
        assertNotNull(client);
        assertEquals("gcp", client.getProviderId());
    }
}
