package com.salesforce.multicloudj.blob.client;

import com.salesforce.multicloudj.blob.driver.AbstractBlobClient;
import com.salesforce.multicloudj.blob.driver.BucketInfo;
import com.salesforce.multicloudj.blob.driver.ListBucketsResponse;
import com.salesforce.multicloudj.blob.driver.TestBlobClient;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.retries.RetryConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.MockedStatic;

import java.net.URI;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BlobClientTest {

    private AbstractBlobClient<TestBlobClient> mockBlobClient;
    private BlobClient client;

    private MockedStatic<ProviderSupplier> providerSupplier;

    @BeforeEach
    void setup() {
        mockBlobClient = mock(AbstractBlobClient.class);
        doReturn(UnAuthorizedException.class).when(mockBlobClient).getException(any());
        providerSupplier = mockStatic(ProviderSupplier.class);
        AbstractBlobClient.Builder mockBuilder = mock(AbstractBlobClient.Builder.class);
        when(mockBuilder.build()).thenReturn(mockBlobClient);
        providerSupplier.when(() -> ProviderSupplier.findBlobClientProviderBuilder("test1")).thenReturn(mockBuilder);
        client = BlobClient.builder("test1")
                .withRegion("us-west-1")
                .withEndpoint(URI.create("https://blob.endpoint.com"))
                .withProxyEndpoint(URI.create("https://proxy.endpoint.com"))
                .build();
    }

    @AfterEach
    void teardown() {
        if (providerSupplier != null) {
            providerSupplier.close();
        }
    }

    @Test
    void testListBuckets() {
        BucketInfo bucket1 = BucketInfo.builder().name("bucket1").build();
        BucketInfo bucket2 = BucketInfo.builder().name("bucket2").build();

        List<BucketInfo> buckets = List.of(bucket1, bucket2);
        ListBucketsResponse expectedResponse = ListBucketsResponse.builder().bucketInfoList(buckets).build();
        when(mockBlobClient.listBuckets()).thenReturn(expectedResponse);


        ListBucketsResponse response = client.listBuckets();
        verify(mockBlobClient, times(1)).listBuckets();
        Assertions.assertEquals(expectedResponse.getBucketInfoList().size(), response.getBucketInfoList().size());
        Assertions.assertEquals(expectedResponse.getBucketInfoList().get(0).getName(), response.getBucketInfoList().get(0).getName());
    }

    @Test
    void testBucketsListThrowsException() {
        doThrow(RuntimeException.class).when(mockBlobClient).listBuckets();

        assertThrows(UnAuthorizedException.class, () -> client.listBuckets());
    }

    @Test
    void testBlobClientBuilderWithRetryConfig() {
        RetryConfig retryConfig = RetryConfig.builder()
                .maxAttempts(5)
                .attemptTimeout(3000L)
                .totalTimeout(10000L)
                .build();

        AbstractBlobClient.Builder mockBuilder2 = mock(AbstractBlobClient.Builder.class);
        when(mockBuilder2.withRegion(any())).thenReturn(mockBuilder2);
        when(mockBuilder2.withRetryConfig(any())).thenReturn(mockBuilder2);
        when(mockBuilder2.build()).thenReturn(mockBlobClient);

        providerSupplier.when(() -> ProviderSupplier.findBlobClientProviderBuilder("test2"))
                .thenReturn(mockBuilder2);

        BlobClient testClient = BlobClient.builder("test2")
                .withRegion("us-east-1")
                .withRetryConfig(retryConfig)
                .build();

        verify(mockBuilder2, times(1)).withRetryConfig(retryConfig);
        Assertions.assertNotNull(testClient);
    }

    @Test
    void testCreateBucket() {
        String bucketName = "test-bucket";

        // Call createBucket
        client.createBucket(bucketName);

        // Verify that createBucket was called on the underlying AbstractBlobClient
        verify(mockBlobClient, times(1)).createBucket(bucketName);
    }

    @Test
    void testCreateBucketThrowsException() {
        String bucketName = "test-bucket";
        doThrow(RuntimeException.class).when(mockBlobClient).createBucket(bucketName);

        // Should throw UnAuthorizedException since mockBlobClient.getException returns UnAuthorizedException.class
        assertThrows(UnAuthorizedException.class, () -> client.createBucket(bucketName));
    }
}
