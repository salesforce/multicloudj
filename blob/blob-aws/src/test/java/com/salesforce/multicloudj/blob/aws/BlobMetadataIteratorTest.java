package com.salesforce.multicloudj.blob.aws;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsResponse;
import software.amazon.awssdk.services.s3.model.ObjectVersion;
import software.amazon.awssdk.services.s3.paginators.ListObjectVersionsIterable;

/** Unit tests for BlobMetadataIterator. */
public class BlobMetadataIteratorTest {

  private static final String TEST_BUCKET = "test-bucket";
  private S3Client mockS3Client;

  @BeforeEach
  void setUp() {
    mockS3Client = mock(S3Client.class);
  }

  @Test
  void testSingleMatchingVersionFiltersPrefixOnlyMatches() {
    String key = "obj-1";
    ObjectVersion matching =
        ObjectVersion.builder()
            .key(key)
            .versionId("v1")
            .eTag("etag-v1")
            .size(123L)
            .lastModified(Instant.now())
            .build();
    ObjectVersion nonMatching =
        ObjectVersion.builder()
            .key("obj-1-extra")
            .versionId("v2")
            .eTag("etag-v2")
            .size(456L)
            .lastModified(Instant.now())
            .build();

    ListObjectVersionsResponse response =
        ListObjectVersionsResponse.builder().versions(matching, nonMatching).build();

    ListObjectVersionsIterable iterable = mock(ListObjectVersionsIterable.class);
    when(iterable.iterator()).thenReturn(List.of(response).iterator());
    when(mockS3Client.listObjectVersionsPaginator(any(ListObjectVersionsRequest.class)))
        .thenReturn(iterable);

    Iterator<BlobMetadata> iterator = new BlobMetadataIterator(mockS3Client, TEST_BUCKET, key);

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

    ListObjectVersionsResponse page1 =
        ListObjectVersionsResponse.builder().versions(version1).build();
    ListObjectVersionsResponse page2 =
        ListObjectVersionsResponse.builder().versions(version2, version3).build();

    ListObjectVersionsIterable iterable = mock(ListObjectVersionsIterable.class);
    when(iterable.iterator()).thenReturn(List.of(page1, page2).iterator());
    when(mockS3Client.listObjectVersionsPaginator(any(ListObjectVersionsRequest.class)))
        .thenReturn(iterable);

    Iterator<BlobMetadata> iterator = new BlobMetadataIterator(mockS3Client, TEST_BUCKET, key);
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
    ListObjectVersionsResponse emptyResponse =
        ListObjectVersionsResponse.builder().versions(List.of()).build();

    ListObjectVersionsIterable iterable = mock(ListObjectVersionsIterable.class);
    when(iterable.iterator()).thenReturn(List.of(emptyResponse).iterator());
    when(mockS3Client.listObjectVersionsPaginator(any(ListObjectVersionsRequest.class)))
        .thenReturn(iterable);

    Iterator<BlobMetadata> iterator = new BlobMetadataIterator(mockS3Client, TEST_BUCKET, key);
    assertFalse(iterator.hasNext());
  }

  private static ObjectVersion version(String key, String versionId, long size) {
    return ObjectVersion.builder()
        .key(key)
        .versionId(versionId)
        .eTag("etag-" + versionId)
        .size(size)
        .lastModified(Instant.now())
        .build();
  }
}
