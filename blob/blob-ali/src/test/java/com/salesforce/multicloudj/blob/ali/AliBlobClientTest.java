package com.salesforce.multicloudj.blob.ali;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.salesforce.multicloudj.blob.driver.ListBucketsResponse;
import com.salesforce.multicloudj.common.ali.AliConstants;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.net.URI;
import java.time.Instant;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AliBlobClientTest {

    private MockedStatic<OSSClientBuilder> staticMockBuilder;

    private OSS mockOssClient;
    private AliBlobClient ali;

    @BeforeEach
    void setup() {
        mockOssClient = mock(OSS.class);
        staticMockBuilder = mockStatic(OSSClientBuilder.class);
        OSSClientBuilder.OSSClientBuilderImpl mockBuilder = mock(OSSClientBuilder.OSSClientBuilderImpl.class);

        staticMockBuilder.when(OSSClientBuilder::create).thenReturn(mockBuilder);
        when(mockBuilder.endpoint(any())).thenReturn(mockBuilder);
        when(mockBuilder.credentialsProvider(any())).thenReturn(mockBuilder);
        when(mockBuilder.build()).thenReturn(mockOssClient);

        StsCredentials creds = new StsCredentials("key-1", "secret-1", "token-1");
        CredentialsOverrider credsOverrider = new CredentialsOverrider.Builder(CredentialsType.SESSION)
                .withSessionCredentials(creds).build();
        ali = new AliBlobClient.Builder().withRegion("us-east")
                .withEndpoint(URI.create("https://test.example.com"))
                .withProxyEndpoint(URI.create("http://proxy.example.com:80"))
                .withCredentialsOverrider(credsOverrider)
                .build();
    }

    @AfterEach
    void teardown() {
        if (staticMockBuilder != null) {
            staticMockBuilder.close();
        }
    }

    @Test
    void testDoListBuckets() {
        // Prepare mock data list of buckets
        Date date = Date.from(Instant.now());
        com.aliyun.oss.model.Bucket bucket1 = new com.aliyun.oss.model.Bucket("bucket1");
        com.aliyun.oss.model.Bucket bucket2 = new com.aliyun.oss.model.Bucket("bucket2");
        bucket1.setCreationDate(date);
        bucket2.setCreationDate(date);
        List<com.aliyun.oss.model.Bucket> buckets = List.of(bucket1, bucket2);
        when(mockOssClient.listBuckets()).thenReturn(buckets);

        ListBucketsResponse response = ali.listBuckets();

        verify(mockOssClient).listBuckets();

        assertNotNull(response);
        assertEquals(2, response.getBucketInfoList().size());
        assertEquals("bucket1", response.getBucketInfoList().get(0).getName());
        assertEquals("bucket2", response.getBucketInfoList().get(1).getName());
    }

    @Test
    void testProviderId() {
        assertEquals(AliConstants.PROVIDER_ID, ali.getProviderId());
    }
}
