package com.salesforce.multicloudj.blob.aws;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import software.amazon.awssdk.services.s3.model.DeleteMarkerEntry;
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

  @Test
  void testDeleteMarkersEmittedAndMergedWhenIncluded() {
    String key = "obj-1";
    Instant t1 = Instant.parse("2024-01-01T00:00:00Z");
    Instant t2 = Instant.parse("2024-01-02T00:00:00Z");
    Instant t3 = Instant.parse("2024-01-03T00:00:00Z");

    // S3 returns versions and delete markers in two separate newest-first collections.
    // Timeline newest-first: v2 (t3) -> marker (t2) -> v1 (t1).
    ObjectVersion v1 = version(key, "v1", 100L, t1);
    ObjectVersion v2 = version(key, "v2", 200L, t3);
    DeleteMarkerEntry marker = marker(key, "dm1", t2);

    ListObjectVersionsResponse response =
        ListObjectVersionsResponse.builder().versions(v2, v1).deleteMarkers(marker).build();

    ListObjectVersionsIterable iterable = mock(ListObjectVersionsIterable.class);
    when(iterable.iterator()).thenReturn(List.of(response).iterator());
    when(mockS3Client.listObjectVersionsPaginator(any(ListObjectVersionsRequest.class)))
        .thenReturn(iterable);

    Iterator<BlobMetadata> iterator =
        new BlobMetadataIterator(mockS3Client, TEST_BUCKET, key, true);
    List<BlobMetadata> all = new ArrayList<>();
    iterator.forEachRemaining(all::add);

    assertEquals(3, all.size());

    // Newest content version: current, so no supersession instant.
    BlobMetadata current = all.get(0);
    assertEquals("v2", current.getVersionId());
    assertFalse(current.isDeleteMarker());
    assertNull(current.getNoncurrentAt());

    // Delete marker sits between v2 and v1; it stopped being current when v2 was created.
    BlobMetadata dm = all.get(1);
    assertEquals("dm1", dm.getVersionId());
    assertTrue(dm.isDeleteMarker());
    assertEquals(t2, dm.getCreatedTime());
    assertEquals(t3, dm.getNoncurrentAt());

    // Oldest content version stopped being current when the delete marker was created.
    BlobMetadata oldest = all.get(2);
    assertEquals("v1", oldest.getVersionId());
    assertFalse(oldest.isDeleteMarker());
    assertEquals(t1, oldest.getCreatedTime());
    assertEquals(t2, oldest.getNoncurrentAt());
  }

  @Test
  void testDeleteMarkersFilteredButStillDriveNoncurrentAt() {
    String key = "obj-1";
    Instant t1 = Instant.parse("2024-01-01T00:00:00Z");
    Instant t2 = Instant.parse("2024-01-02T00:00:00Z");
    Instant t3 = Instant.parse("2024-01-03T00:00:00Z");

    ObjectVersion v1 = version(key, "v1", 100L, t1);
    ObjectVersion v2 = version(key, "v2", 200L, t3);
    DeleteMarkerEntry marker = marker(key, "dm1", t2);

    ListObjectVersionsResponse response =
        ListObjectVersionsResponse.builder().versions(v2, v1).deleteMarkers(marker).build();

    ListObjectVersionsIterable iterable = mock(ListObjectVersionsIterable.class);
    when(iterable.iterator()).thenReturn(List.of(response).iterator());
    when(mockS3Client.listObjectVersionsPaginator(any(ListObjectVersionsRequest.class)))
        .thenReturn(iterable);

    // includeDeleteMarkers defaults to false via the 3-arg constructor.
    Iterator<BlobMetadata> iterator = new BlobMetadataIterator(mockS3Client, TEST_BUCKET, key);
    List<BlobMetadata> all = new ArrayList<>();
    iterator.forEachRemaining(all::add);

    // Only content versions are emitted, but the older version's noncurrentAt must still
    // reflect that a delete marker (not the newer version) superseded it.
    assertEquals(2, all.size());
    assertEquals("v2", all.get(0).getVersionId());
    assertFalse(all.get(0).isDeleteMarker());
    assertNull(all.get(0).getNoncurrentAt());

    assertEquals("v1", all.get(1).getVersionId());
    assertFalse(all.get(1).isDeleteMarker());
    assertEquals(t2, all.get(1).getNoncurrentAt());
  }

  @Test
  void testMultiplePagesMergeDeleteMarkersAcrossPageBoundaries() {
    String key = "obj-1";
    Instant t2 = Instant.parse("2024-01-02T00:00:00Z");
    Instant t4 = Instant.parse("2024-01-04T00:00:00Z");
    Instant t5 = Instant.parse("2024-01-05T00:00:00Z");

    // Global newest-first sequence for the key: v3 (t5) -> marker (t4) -> v1 (t2), split so that
    // the marker on page 1 must merge ahead of the version on page 2.
    ObjectVersion v3 = version(key, "v3", 300L, t5);
    DeleteMarkerEntry marker = marker(key, "dm1", t4);
    ObjectVersion v1 = version(key, "v1", 100L, t2);

    ListObjectVersionsResponse page1 =
        ListObjectVersionsResponse.builder().versions(v3).deleteMarkers(marker).build();
    ListObjectVersionsResponse page2 =
        ListObjectVersionsResponse.builder().versions(v1).build();

    ListObjectVersionsIterable iterable = mock(ListObjectVersionsIterable.class);
    when(iterable.iterator()).thenReturn(List.of(page1, page2).iterator());
    when(mockS3Client.listObjectVersionsPaginator(any(ListObjectVersionsRequest.class)))
        .thenReturn(iterable);

    Iterator<BlobMetadata> iterator =
        new BlobMetadataIterator(mockS3Client, TEST_BUCKET, key, true);
    List<BlobMetadata> all = new ArrayList<>();
    iterator.forEachRemaining(all::add);

    assertEquals(3, all.size());

    assertEquals("v3", all.get(0).getVersionId());
    assertFalse(all.get(0).isDeleteMarker());
    assertNull(all.get(0).getNoncurrentAt());

    // Delete marker from page 1 merges ahead of the version from page 2.
    assertEquals("dm1", all.get(1).getVersionId());
    assertTrue(all.get(1).isDeleteMarker());
    assertEquals(t4, all.get(1).getCreatedTime());
    assertEquals(t5, all.get(1).getNoncurrentAt());

    // Cross-page supersession: the older version stopped being current at the marker instant.
    assertEquals("v1", all.get(2).getVersionId());
    assertFalse(all.get(2).isDeleteMarker());
    assertEquals(t2, all.get(2).getCreatedTime());
    assertEquals(t4, all.get(2).getNoncurrentAt());
  }

  @Test
  void testMarkerOnlyPageFlagOn() {
    String key = "obj-1";
    Instant t1 = Instant.parse("2024-01-01T00:00:00Z");
    DeleteMarkerEntry marker = marker(key, "dm1", t1);

    ListObjectVersionsResponse response =
        ListObjectVersionsResponse.builder().deleteMarkers(marker).build();

    ListObjectVersionsIterable iterable = mock(ListObjectVersionsIterable.class);
    when(iterable.iterator()).thenReturn(List.of(response).iterator());
    when(mockS3Client.listObjectVersionsPaginator(any(ListObjectVersionsRequest.class)))
        .thenReturn(iterable);

    Iterator<BlobMetadata> iterator =
        new BlobMetadataIterator(mockS3Client, TEST_BUCKET, key, true);
    List<BlobMetadata> all = new ArrayList<>();
    iterator.forEachRemaining(all::add);

    assertEquals(1, all.size());
    assertEquals("dm1", all.get(0).getVersionId());
    assertTrue(all.get(0).isDeleteMarker());
    assertNull(all.get(0).getNoncurrentAt());
  }

  @Test
  void testMarkerOnlyPageFlagOff() {
    String key = "obj-1";
    Instant t1 = Instant.parse("2024-01-01T00:00:00Z");
    DeleteMarkerEntry marker = marker(key, "dm1", t1);

    ListObjectVersionsResponse response =
        ListObjectVersionsResponse.builder().deleteMarkers(marker).build();

    ListObjectVersionsIterable iterable = mock(ListObjectVersionsIterable.class);
    when(iterable.iterator()).thenReturn(List.of(response).iterator());
    when(mockS3Client.listObjectVersionsPaginator(any(ListObjectVersionsRequest.class)))
        .thenReturn(iterable);

    // Default 3-arg constructor hides delete markers.
    Iterator<BlobMetadata> iterator = new BlobMetadataIterator(mockS3Client, TEST_BUCKET, key);
    assertFalse(iterator.hasNext());
  }

  @Test
  void testEmptyFilteredPageThenLaterPageWithData() {
    String key = "obj-1";
    // Page 1 holds only a sibling key that shares the prefix; it filters to empty.
    ObjectVersion sibling = version("obj-1-extra", "vX", 999L);
    ObjectVersion matching = version(key, "v1", 100L);

    ListObjectVersionsResponse page1 =
        ListObjectVersionsResponse.builder().versions(sibling).build();
    ListObjectVersionsResponse page2 =
        ListObjectVersionsResponse.builder().versions(matching).build();

    ListObjectVersionsIterable iterable = mock(ListObjectVersionsIterable.class);
    when(iterable.iterator()).thenReturn(List.of(page1, page2).iterator());
    when(mockS3Client.listObjectVersionsPaginator(any(ListObjectVersionsRequest.class)))
        .thenReturn(iterable);

    Iterator<BlobMetadata> iterator = new BlobMetadataIterator(mockS3Client, TEST_BUCKET, key);
    List<BlobMetadata> all = new ArrayList<>();
    iterator.forEachRemaining(all::add);

    assertEquals(1, all.size());
    assertEquals(key, all.get(0).getKey());
    assertEquals("v1", all.get(0).getVersionId());
  }

  @Test
  void testLazyPageLoadingPullsPagesOnDemand() {
    String key = "obj-1";
    ListObjectVersionsResponse page1 =
        ListObjectVersionsResponse.builder().versions(version(key, "v3", 300L)).build();
    ListObjectVersionsResponse page2 =
        ListObjectVersionsResponse.builder().versions(version(key, "v2", 200L)).build();
    ListObjectVersionsResponse page3 =
        ListObjectVersionsResponse.builder().versions(version(key, "v1", 100L)).build();

    int[] fetched = {0};
    Iterator<ListObjectVersionsResponse> counting =
        new Iterator<>() {
          private final Iterator<ListObjectVersionsResponse> delegate =
              List.of(page1, page2, page3).iterator();

          @Override
          public boolean hasNext() {
            return delegate.hasNext();
          }

          @Override
          public ListObjectVersionsResponse next() {
            fetched[0]++;
            return delegate.next();
          }
        };

    ListObjectVersionsIterable iterable = mock(ListObjectVersionsIterable.class);
    when(iterable.iterator()).thenReturn(counting);
    when(mockS3Client.listObjectVersionsPaginator(any(ListObjectVersionsRequest.class)))
        .thenReturn(iterable);

    Iterator<BlobMetadata> iterator = new BlobMetadataIterator(mockS3Client, TEST_BUCKET, key);

    // Construction alone fetches nothing.
    assertEquals(0, fetched[0]);

    // Reading the first element pulls only the first page.
    assertTrue(iterator.hasNext());
    assertEquals("v3", iterator.next().getVersionId());
    assertEquals(1, fetched[0]);

    // Draining the rest pulls the remaining pages on demand.
    List<BlobMetadata> rest = new ArrayList<>();
    iterator.forEachRemaining(rest::add);
    assertEquals(2, rest.size());
    assertEquals(3, fetched[0]);
  }

  @Test
  void testEqualTimestampsBothEntriesPresent() {
    String key = "obj-1";
    Instant t = Instant.parse("2024-01-01T00:00:00Z");
    ObjectVersion v1 = version(key, "v1", 100L, t);
    DeleteMarkerEntry marker = marker(key, "dm1", t);

    ListObjectVersionsResponse response =
        ListObjectVersionsResponse.builder().versions(v1).deleteMarkers(marker).build();

    ListObjectVersionsIterable iterable = mock(ListObjectVersionsIterable.class);
    when(iterable.iterator()).thenReturn(List.of(response).iterator());
    when(mockS3Client.listObjectVersionsPaginator(any(ListObjectVersionsRequest.class)))
        .thenReturn(iterable);

    Iterator<BlobMetadata> iterator =
        new BlobMetadataIterator(mockS3Client, TEST_BUCKET, key, true);
    List<BlobMetadata> all = new ArrayList<>();
    iterator.forEachRemaining(all::add);

    // Both entries surface; equal-timestamp ordering is intentionally left unasserted.
    assertEquals(2, all.size());
    long markerCount = all.stream().filter(BlobMetadata::isDeleteMarker).count();
    assertEquals(1, markerCount);
  }

  private static ObjectVersion version(String key, String versionId, long size) {
    return version(key, versionId, size, Instant.now());
  }

  private static ObjectVersion version(
      String key, String versionId, long size, Instant lastModified) {
    return ObjectVersion.builder()
        .key(key)
        .versionId(versionId)
        .eTag("etag-" + versionId)
        .size(size)
        .lastModified(lastModified)
        .build();
  }

  private static DeleteMarkerEntry marker(String key, String versionId, Instant lastModified) {
    return DeleteMarkerEntry.builder()
        .key(key)
        .versionId(versionId)
        .lastModified(lastModified)
        .build();
  }
}
