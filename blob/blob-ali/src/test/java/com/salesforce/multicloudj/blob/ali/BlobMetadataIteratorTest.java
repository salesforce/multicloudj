package com.salesforce.multicloudj.blob.ali;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.aliyun.sdk.service.oss2.OSSClient;
import com.aliyun.sdk.service.oss2.models.DeleteMarkerEntry;
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

    stubPages(result);

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
    ObjectVersion version1 = version(key, "v1", 100L, Instant.ofEpochSecond(300));
    ObjectVersion version2 = version(key, "v2", 200L, Instant.ofEpochSecond(200));
    ObjectVersion version3 = version(key, "v3", 300L, Instant.ofEpochSecond(100));

    ListObjectVersionsResult page1 = mock(ListObjectVersionsResult.class);
    when(page1.versions()).thenReturn(List.of(version1));

    ListObjectVersionsResult page2 = mock(ListObjectVersionsResult.class);
    when(page2.versions()).thenReturn(List.of(version2, version3));

    stubPages(page1, page2);

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

    stubPages(emptyResult);

    Iterator<BlobMetadata> iterator =
        new BlobMetadataIterator(mockOssClient, TEST_BUCKET, key);
    assertFalse(iterator.hasNext());
  }

  @Test
  void testMergeDeleteMarkersAcrossPagesFlagOn() {
    String key = "obj-1";
    // Global newest-first timeline for the key: v3(t5) -> marker(t4) -> v1(t2).
    ObjectVersion v3 = version(key, "v3", 300L, Instant.ofEpochSecond(5));
    DeleteMarkerEntry marker = marker(key, "dm", Instant.ofEpochSecond(4), false);
    ObjectVersion v1 = version(key, "v1", 100L, Instant.ofEpochSecond(2));

    ListObjectVersionsResult page1 = mock(ListObjectVersionsResult.class);
    when(page1.versions()).thenReturn(List.of(v3));
    when(page1.deleteMarkers()).thenReturn(List.of(marker));

    ListObjectVersionsResult page2 = mock(ListObjectVersionsResult.class);
    when(page2.versions()).thenReturn(List.of(v1));

    stubPages(page1, page2);

    List<BlobMetadata> all = new ArrayList<>();
    new BlobMetadataIterator(mockOssClient, TEST_BUCKET, key, true).forEachRemaining(all::add);

    assertEquals(3, all.size());

    assertEquals("v3", all.get(0).getVersionId());
    assertFalse(all.get(0).isArchived());
    assertNull(all.get(0).getArchivedAt(), "Newest entry is still current");

    assertEquals("dm", all.get(1).getVersionId());
    assertTrue(all.get(1).isArchived());
    assertEquals(Instant.ofEpochSecond(5), all.get(1).getArchivedAt());

    assertEquals("v1", all.get(2).getVersionId());
    assertFalse(all.get(2).isArchived());
    assertEquals(Instant.ofEpochSecond(4), all.get(2).getArchivedAt());
  }

  @Test
  void testMarkersHiddenAndNoArchivedAtWhenFlagOff() {
    String key = "obj-1";
    // Timeline: v2(t3) -> marker(t2) -> v1(t1). With the flag off, the marker is neither surfaced
    // nor used to derive a supersession instant, so no version reports archivedAt.
    ObjectVersion v2 = version(key, "v2", 200L, Instant.ofEpochSecond(3));
    DeleteMarkerEntry marker = marker(key, "dm", Instant.ofEpochSecond(2), false);
    ObjectVersion v1 = version(key, "v1", 100L, Instant.ofEpochSecond(1));

    ListObjectVersionsResult page = mock(ListObjectVersionsResult.class);
    when(page.versions()).thenReturn(List.of(v2, v1));
    when(page.deleteMarkers()).thenReturn(List.of(marker));

    stubPages(page);

    List<BlobMetadata> all = new ArrayList<>();
    new BlobMetadataIterator(mockOssClient, TEST_BUCKET, key, false).forEachRemaining(all::add);

    assertEquals(2, all.size());
    assertTrue(
        all.stream().noneMatch(BlobMetadata::isArchived), "Markers hidden when flag off");
    assertEquals("v2", all.get(0).getVersionId());
    assertNull(all.get(0).getArchivedAt());
    assertEquals("v1", all.get(1).getVersionId());
    assertNull(all.get(1).getArchivedAt(),
        "Default listing must not derive archivedAt when delete markers are not requested");
  }

  @Test
  void testMarkerOnlyPageFlagOn() {
    String key = "obj-1";
    DeleteMarkerEntry marker = marker(key, "dm", Instant.ofEpochSecond(1), true);

    ListObjectVersionsResult page = mock(ListObjectVersionsResult.class);
    when(page.versions()).thenReturn(List.of());
    when(page.deleteMarkers()).thenReturn(List.of(marker));

    stubPages(page);

    List<BlobMetadata> all = new ArrayList<>();
    new BlobMetadataIterator(mockOssClient, TEST_BUCKET, key, true).forEachRemaining(all::add);

    assertEquals(1, all.size());
    assertTrue(all.get(0).isArchived());
    assertEquals("dm", all.get(0).getVersionId());
    assertNull(all.get(0).getArchivedAt());
  }

  @Test
  void testMarkerOnlyPageFlagOff() {
    String key = "obj-1";
    DeleteMarkerEntry marker = marker(key, "dm", Instant.ofEpochSecond(1), true);

    ListObjectVersionsResult page = mock(ListObjectVersionsResult.class);
    when(page.versions()).thenReturn(List.of());
    when(page.deleteMarkers()).thenReturn(List.of(marker));

    stubPages(page);

    Iterator<BlobMetadata> iterator = new BlobMetadataIterator(mockOssClient, TEST_BUCKET, key);
    assertFalse(iterator.hasNext());
  }

  @Test
  void testEmptyFilteredPageThenLaterPageWithData() {
    String key = "obj-1";
    // Page 1 only holds a sibling key that shares the prefix; it filters to empty.
    ObjectVersion sibling = version("obj-1-extra", "vX", 999L, Instant.ofEpochSecond(9));
    ObjectVersion matching = version(key, "v1", 100L, Instant.ofEpochSecond(1));

    ListObjectVersionsResult page1 = mock(ListObjectVersionsResult.class);
    when(page1.versions()).thenReturn(List.of(sibling));

    ListObjectVersionsResult page2 = mock(ListObjectVersionsResult.class);
    when(page2.versions()).thenReturn(List.of(matching));

    stubPages(page1, page2);

    List<BlobMetadata> all = new ArrayList<>();
    new BlobMetadataIterator(mockOssClient, TEST_BUCKET, key).forEachRemaining(all::add);

    assertEquals(1, all.size());
    assertEquals("v1", all.get(0).getVersionId());
  }

  @Test
  void testEqualTimestampsBothEntriesPresent() {
    String key = "obj-1";
    Instant same = Instant.ofEpochSecond(7);
    ObjectVersion v = version(key, "v1", 100L, same);
    DeleteMarkerEntry marker = marker(key, "dm", same, true);

    ListObjectVersionsResult page = mock(ListObjectVersionsResult.class);
    when(page.versions()).thenReturn(List.of(v));
    when(page.deleteMarkers()).thenReturn(List.of(marker));

    stubPages(page);

    List<BlobMetadata> all = new ArrayList<>();
    new BlobMetadataIterator(mockOssClient, TEST_BUCKET, key, true).forEachRemaining(all::add);

    // Equal-timestamp cross-type order is unspecified; assert only that both entries survive.
    assertEquals(2, all.size());
    assertTrue(all.stream().anyMatch(m -> "v1".equals(m.getVersionId())));
    assertTrue(all.stream().anyMatch(m -> "dm".equals(m.getVersionId())));
  }

  @Test
  void testEqualTimestampsLatestFlagBreaksTie() {
    String key = "obj-1";
    Instant t = Instant.ofEpochSecond(7);

    // Same-instant PUT then DELETE: the delete marker is the current (latest) entry, so the latest
    // flag must decide the order that the equal timestamps cannot.
    ObjectVersion v1 = version(key, "v1", 100L, t);
    DeleteMarkerEntry marker = marker(key, "dm1", t, true);

    ListObjectVersionsResult page = mock(ListObjectVersionsResult.class);
    when(page.versions()).thenReturn(List.of(v1));
    when(page.deleteMarkers()).thenReturn(List.of(marker));

    stubPages(page);

    List<BlobMetadata> all = new ArrayList<>();
    new BlobMetadataIterator(mockOssClient, TEST_BUCKET, key, true).forEachRemaining(all::add);

    assertEquals(2, all.size());

    // The delete marker OSS flagged latest is emitted first and is still current.
    assertEquals("dm1", all.get(0).getVersionId());
    assertTrue(all.get(0).isArchived());
    assertNull(all.get(0).getArchivedAt());

    // The content version sharing the instant is superseded by that marker.
    assertEquals("v1", all.get(1).getVersionId());
    assertFalse(all.get(1).isArchived());
    assertEquals(t, all.get(1).getArchivedAt());
  }

  @Test
  void testLazyPageLoadingPullsPagesOnDemand() {
    String key = "obj-1";
    // Build the version mocks first: stubbing them inside a thenReturn(...) argument would nest
    // stubbing calls and trip Mockito's UnfinishedStubbingException.
    ObjectVersion v3 = version(key, "v3", 300L, Instant.ofEpochSecond(3));
    ObjectVersion v2 = version(key, "v2", 200L, Instant.ofEpochSecond(2));
    ObjectVersion v1 = version(key, "v1", 100L, Instant.ofEpochSecond(1));
    ListObjectVersionsResult page1 = mock(ListObjectVersionsResult.class);
    when(page1.versions()).thenReturn(List.of(v3));
    ListObjectVersionsResult page2 = mock(ListObjectVersionsResult.class);
    when(page2.versions()).thenReturn(List.of(v2));
    ListObjectVersionsResult page3 = mock(ListObjectVersionsResult.class);
    when(page3.versions()).thenReturn(List.of(v1));

    int[] fetched = {0};
    Iterator<ListObjectVersionsResult> counting =
        new Iterator<>() {
          private final Iterator<ListObjectVersionsResult> delegate =
              List.of(page1, page2, page3).iterator();

          @Override
          public boolean hasNext() {
            return delegate.hasNext();
          }

          @Override
          public ListObjectVersionsResult next() {
            fetched[0]++;
            return delegate.next();
          }
        };

    ListObjectVersionsIterable iterable = mock(ListObjectVersionsIterable.class);
    when(iterable.iterator()).thenReturn(counting);
    when(mockOssClient.listObjectVersionsPaginator(any(ListObjectVersionsRequest.class)))
        .thenReturn(iterable);

    Iterator<BlobMetadata> iterator = new BlobMetadataIterator(mockOssClient, TEST_BUCKET, key);

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

  private void stubPages(ListObjectVersionsResult... pages) {
    ListObjectVersionsIterable iterable = mock(ListObjectVersionsIterable.class);
    when(iterable.iterator()).thenReturn(List.of(pages).iterator());
    when(mockOssClient.listObjectVersionsPaginator(
        any(ListObjectVersionsRequest.class))).thenReturn(iterable);
  }

  private static ObjectVersion version(String key, String versionId, long size) {
    return version(key, versionId, size, Instant.now());
  }

  private static ObjectVersion version(
      String key, String versionId, long size, Instant lastModified) {
    ObjectVersion v = mock(ObjectVersion.class);
    when(v.key()).thenReturn(key);
    when(v.versionId()).thenReturn(versionId);
    when(v.eTag()).thenReturn("etag-" + versionId);
    when(v.size()).thenReturn(size);
    when(v.lastModified()).thenReturn(lastModified);
    return v;
  }

  private static DeleteMarkerEntry marker(
      String key, String versionId, Instant lastModified, boolean isLatest) {
    DeleteMarkerEntry m = mock(DeleteMarkerEntry.class);
    when(m.key()).thenReturn(key);
    when(m.versionId()).thenReturn(versionId);
    when(m.lastModified()).thenReturn(lastModified);
    when(m.isLatest()).thenReturn(isLatest);
    return m;
  }
}
