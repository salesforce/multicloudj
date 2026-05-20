package com.salesforce.multicloudj.blob.ali;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.aliyun.oss.OSS;
import com.aliyun.sdk.service.oss2.OSSClient;
import com.aliyun.sdk.service.oss2.OperationOptions;
import com.aliyun.sdk.service.oss2.models.BucketSummary;
import com.aliyun.sdk.service.oss2.models.ListBucketsRequest;
import com.aliyun.sdk.service.oss2.models.ListBucketsResult;
import com.aliyun.sdk.service.oss2.models.PutBucketRequest;
import com.aliyun.sdk.service.oss2.models.PutBucketResult;
import com.salesforce.multicloudj.blob.driver.ListBucketsResponse;
import com.salesforce.multicloudj.common.ali.AliConstants;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.net.URI;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AliBlobClientTest {

  private OSS mockOssClient;
  private OSSClient mockOssV2Client;
  private AliBlobClient ali;

  @BeforeEach
  void setup() {
    mockOssClient = mock(OSS.class);
    mockOssV2Client = mock(OSSClient.class);

    StsCredentials creds = new StsCredentials("key-1", "secret-1", "token-1");
    CredentialsOverrider credsOverrider =
        new CredentialsOverrider.Builder(CredentialsType.SESSION)
            .withSessionCredentials(creds)
            .build();
    AliBlobClient.Builder builder = new AliBlobClient.Builder();
    builder.withRegion("us-east")
        .withEndpoint(URI.create("https://test.example.com"))
        .withProxyEndpoint(URI.create("http://proxy.example.com:80"))
        .withCredentialsOverrider(credsOverrider);
    ali = builder.build(mockOssClient, mockOssV2Client);
  }

  @Test
  void testDoListBuckets() {
    Instant now = Instant.now();
    BucketSummary bucket1 = BucketSummary.newBuilder()
        .name("bucket1").region("cn-shanghai").creationDate(now).build();
    BucketSummary bucket2 = BucketSummary.newBuilder()
        .name("bucket2").region("cn-shanghai").creationDate(now).build();
    ListBucketsResult result = ListBucketsResult.newBuilder().build();
    when(mockOssV2Client.listBuckets(any(ListBucketsRequest.class), any(OperationOptions.class)))
        .thenReturn(result);
    // Use reflection or spy to set buckets — but ListBucketsResult uses internal XML delegate.
    // Instead, mock at the OSSClient level and verify the call is made correctly.
    when(mockOssV2Client.listBuckets(any(ListBucketsRequest.class), any(OperationOptions.class)))
        .thenAnswer(invocation -> {
          ListBucketsResult mockResult = mock(ListBucketsResult.class);
          when(mockResult.buckets()).thenReturn(List.of(bucket1, bucket2));
          return mockResult;
        });

    ListBucketsResponse response = ali.listBuckets();

    verify(mockOssV2Client).listBuckets(any(ListBucketsRequest.class), any(OperationOptions.class));

    assertNotNull(response);
    assertEquals(2, response.getBucketInfoList().size());
    assertEquals("bucket1", response.getBucketInfoList().get(0).getName());
    assertEquals("bucket2", response.getBucketInfoList().get(1).getName());
    assertEquals("cn-shanghai", response.getBucketInfoList().get(0).getRegion());
    assertEquals(now, response.getBucketInfoList().get(0).getCreationDate());
  }

  @Test
  void testProviderId() {
    assertEquals(AliConstants.PROVIDER_ID, ali.getProviderId());
  }

  @Test
  void testCreateBucket() {
    String bucketName = "test-bucket";
    when(mockOssV2Client.putBucket(any(PutBucketRequest.class), any(OperationOptions.class)))
        .thenReturn(PutBucketResult.newBuilder().build());

    ali.createBucket(bucketName);

    verify(mockOssV2Client).putBucket(any(PutBucketRequest.class), any(OperationOptions.class));
  }

  @Test
  void testClose() throws Exception {
    ali.close();
    verify(mockOssClient).shutdown();
    verify(mockOssV2Client).close();
  }
}
