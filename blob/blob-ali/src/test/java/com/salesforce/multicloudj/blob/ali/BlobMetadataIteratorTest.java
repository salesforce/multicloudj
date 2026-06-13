package com.salesforce.multicloudj.blob.ali;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.aliyun.sdk.service.oss2.OSSClient;
import com.aliyun.sdk.service.oss2.models.ListObjectVersionsRequest;
import com.aliyun.sdk.service.oss2.models.ListObjectVersionsResult;
import com.aliyun.sdk.service.oss2.models.ObjectVersion;
import com.aliyun.sdk.service.oss2.paginator.ListObjectVersionsIterable;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for BlobMetadataIterator. */
public class BlobMetadataIteratorTest {

  private static final String TEST_BUCKET = "test-bucket";
  private OSSClient mockOssClient;

  @BeforeEach
  void setUp() {
    mockOssClient = mock(OSSClient.class);
  }

  @Test
  void testSingleMatchingVersionFiltersPrefixOnlyMatches() {
    String key = "obj-1";
    ObjectVersion matching = version(key, "v1", 123L);
    ObjectVersion nonMatching = version("obj-1-extra", "v2", 456L);

    ListObjectVersionsResult result = mock(ListObjectVersionsResult.class);
    when(result.versions()).thenReturn(List.of(matching, nonMatching));

    ListObjectVersionsIterable iterable = mock(ListObjectVersionsIterable.class);
    when(iterable.iterator()).thenReturn(List.of(result).iterator());
    when(mockOssClient.listObjectVersionsPaginator(
        any(ListObjectVersionsRequest.class))).thenReturn(iterable);

    Iterator<BlobMetadata> iterator =
        new BlobMetadataIterator(mockOssClient, TEST_BUCKET, key);

    assertTrue(iterator.hasNext());
    BlobMetadata metadata = iterator.next();
    assertEquals(key, metadata.getKey());
    assertEquals("v1", metadata.getVersionId());
    assertEquals("etag-v1", metadata.getETag());
    assertEquals(123L, metadata.getObjectSize());
    assertFalse(iterator.hasNext());
  }

  @Test
  void testMultiplePages() {
    String key = "obj-1";
    ObjectVersion version1 = version(key, "v1", 100L);
    ObjectVersion version2 = version(key, "v2", 200L);
    ObjectVersion version3 = version(key, "v3", 300L);

    ListObjectVersionsResult page1 = mock(ListObjectVersionsResult.class);
    when(page1.versions()).thenReturn(List.of(version1));

    ListObjectVersionsResult page2 = mock(ListObjectVersionsResult.class);
    when(page2.versions()).thenReturn(List.of(version2, version3));

    ListObjectVersionsIterable iterable = mock(ListObjectVersionsIterable.class);
    when(iterable.iterator()).thenReturn(List.of(page1, page2).iterator());
    when(mockOssClient.listObjectVersionsPaginator(
        any(ListObjectVersionsRequest.class))).thenReturn(iterable);

    Iterator<BlobMetadata> iterator =
        new BlobMetadataIterator(mockOssClient, TEST_BUCKET, key);
    List<BlobMetadata> all = new ArrayList<>();
    iterator.forEachRemaining(all::add);

    assertEquals(3, all.size());
    assertEquals("v1", all.get(0).getVersionId());
    assertEquals("v2", all.get(1).getVersionId());
    assertEquals("v3", all.get(2).getVersionId());
  }

  @Test
  void testEmptyResult() {
    String key = "obj-1";
    ListObjectVersionsResult emptyResult = mock(ListObjectVersionsResult.class);
    when(emptyResult.versions()).thenReturn(List.of());

    ListObjectVersionsIterable iterable = mock(ListObjectVersionsIterable.class);
    when(iterable.iterator()).thenReturn(List.of(emptyResult).iterator());
    when(mockOssClient.listObjectVersionsPaginator(
        any(ListObjectVersionsRequest.class))).thenReturn(iterable);

    Iterator<BlobMetadata> iterator =
        new BlobMetadataIterator(mockOssClient, TEST_BUCKET, key);
    assertFalse(iterator.hasNext());
  }

  private static ObjectVersion version(String key, String versionId, long size) {
    ObjectVersion v = mock(ObjectVersion.class);
    when(v.key()).thenReturn(key);
    when(v.versionId()).thenReturn(versionId);
    when(v.eTag()).thenReturn("etag-" + versionId);
    when(v.size()).thenReturn(size);
    when(v.lastModified()).thenReturn(Instant.now());
    return v;
  }
}
