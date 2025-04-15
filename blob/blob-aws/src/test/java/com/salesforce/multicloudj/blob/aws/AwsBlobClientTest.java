package com.salesforce.multicloudj.blob.aws;


import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AwsBlobClientTest {

    private MockedStatic<S3Client> s3Client;
    private S3Client mockS3Client;
    private AwsBlobClient aws;

    @BeforeEach
    void setup() {
        var mockBuilder = mock(S3ClientBuilder.class);
        when(mockBuilder.region(any())).thenReturn(mockBuilder);

        s3Client = mockStatic(S3Client.class);
        s3Client.when(S3Client::builder).thenReturn(mockBuilder);

        mockS3Client = mock(S3Client.class);
        when(mockBuilder.build()).thenReturn(mockS3Client);
        when(mockBuilder.credentialsProvider(any())).thenReturn(mockBuilder);
        StsCredentials sessionCreds = new StsCredentials("key-1", "secret-1", "token-1");
        CredentialsOverrider credsOverrider = new CredentialsOverrider.Builder(CredentialsType.SESSION)
                .withSessionCredentials(sessionCreds).build();
        aws = new AwsBlobClient.Builder().withCredentialsOverrider(credsOverrider)
                .withRegion("us-west")
                .withEndpoint(URI.create("https://blob.endpoint.com"))
                .withProxyEndpoint(URI.create("https://proxy.endpoint.com:443"))
                .build();
        credsOverrider = new CredentialsOverrider.Builder(CredentialsType.ASSUME_ROLE).withRole("some-role").build();
        aws = new AwsBlobClient.Builder().withCredentialsOverrider(credsOverrider)
                .withRegion("us-east-2").build();
    }

    @AfterEach
    void tearDown() {
        if (s3Client != null) {
            s3Client.close();
        }
    }

    @Test
    void testDoBucketsList() {
        // Prepare mock data list of buckets
        // Create mock Bucket objects using a mocked list of bucket names
        Bucket bucket1 = mock(Bucket.class);
        when(bucket1.name()).thenReturn("bucket1");

        Bucket bucket2 = mock(Bucket.class);
        when(bucket2.name()).thenReturn("bucket2");
        List<Bucket> buckets = List.of(bucket1, bucket2);

        // Prepare the ListBucketsResponse mock
        ListBucketsResponse listBucketsResponse = mock(ListBucketsResponse.class);

        // Mock the listBuckets() method to return a ListBucketsResponse with a list of buckets
        when(listBucketsResponse.buckets()).thenReturn(buckets);
        when(mockS3Client.listBuckets()).thenReturn(listBucketsResponse);


        // Call the method to be tested
        com.salesforce.multicloudj.blob.driver.ListBucketsResponse response = aws.listBuckets();

        // Verify that listBuckets was called on the mock AmazonS3 client
        verify(mockS3Client).listBuckets();

        // Assert that the result matches the expected buckets
        assertNotNull(response);
        assertEquals(2, response.getBucketInfoList().size());
        assertEquals("bucket1", response.getBucketInfoList().get(0).getName());
    }

    @Test
    void testProviderId() {
        assertEquals("aws", aws.getProviderId());
    }


    @Test
    void testExceptionHandling() {
        AwsServiceException awsServiceException = AwsServiceException.builder()
                .awsErrorDetails(
                        AwsErrorDetails.builder()
                                .errorCode("IncompleteSignature")
                                .build())
                .build();
        Class<?> cls = aws.getException(awsServiceException);
        assertEquals(cls, InvalidArgumentException.class);

        SdkClientException sdkClientException = SdkClientException.builder().build();
        cls = aws.getException(sdkClientException);
        assertEquals(cls, InvalidArgumentException.class);

        cls = aws.getException(new IOException("Channel is closed"));
        assertEquals(cls, UnknownException.class);
    }
}
