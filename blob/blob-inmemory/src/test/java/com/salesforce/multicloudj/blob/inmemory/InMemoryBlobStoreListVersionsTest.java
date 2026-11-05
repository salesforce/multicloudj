package com.salesforce.multicloudj.blob.inmemory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.ListBlobVersionsRequest;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link InMemoryBlobStore#doListBlobVersions} covering the delete-marker timeline, the
 * {@code includeArchived} flag, and cloud-neutral {@code archivedAt} computation.
 */
class InMemoryBlobStoreListVersionsTest {

  private static final String BUCKET = "bucket-versions";

  private InMemoryBlobStore store;

  @BeforeEach
  void setUp() {
    InMemoryBlobStore.clearStorage();
    store = new InMemoryBlobStore.Builder().withBucket(BUCKET).withRegion("local").build();
    InMemoryBlobStore.createBucket(BUCKET);
  }

  @AfterEach
  void tearDown() {
    InMemoryBlobStore.clearStorage();
  }

  private String upload(String key, String content) {
    byte[] bytes = content.getBytes();
    return store
        .upload(
            new UploadRequest.Builder().withKey(key).withContentLength(bytes.length).build(),
            new ByteArrayInputStream(bytes))
        .getVersionId();
  }

  private List<BlobMetadata> list(String key, boolean includeArchived) {
    Iterator<BlobMetadata> it =
        store.listBlobVersions(
            ListBlobVersionsRequest.builder()
                .withKey(key)
                .withIncludeArchived(includeArchived)
                .build());
    List<BlobMetadata> out = new ArrayList<>();
    it.forEachRemaining(out::add);
    return out;
  }

  @Test
  void putDeletePut_flagOff_returnsOnlyContentVersionsNewestFirst() {
    String key = "obj";
    String vA = upload(key, "A");
    store.delete(key, null); // unqualified delete creates a delete marker
    String vB = upload(key, "B");

    List<BlobMetadata> versions = list(key, false);

    assertEquals(2, versions.size());
    assertEquals(vB, versions.get(0).getVersionId());
    assertEquals(vA, versions.get(1).getVersionId());
    versions.forEach(v -> assertFalse(v.isArchived()));
  }

  @Test
  void putDeletePut_flagOn_surfacesDeleteMarkerInTimeline() {
    String key = "obj";
    String vA = upload(key, "A");
    store.delete(key, null);
    String vB = upload(key, "B");

    List<BlobMetadata> entries = list(key, true);

    assertEquals(3, entries.size());
    // Newest-first: B (current) -> delete marker -> A.
    assertEquals(vB, entries.get(0).getVersionId());
    assertFalse(entries.get(0).isArchived());

    assertTrue(entries.get(1).isArchived());
    assertNull(entries.get(1).getETag());

    assertEquals(vA, entries.get(2).getVersionId());
    assertFalse(entries.get(2).isArchived());
  }

  @Test
  void archivedAt_isFlagDependent_derivedOnlyWhenMarkersRequested() {
    String key = "obj";
    upload(key, "A");
    store.delete(key, null);
    upload(key, "B");

    List<BlobMetadata> hidden = list(key, false);
    List<BlobMetadata> shown = list(key, true);

    // Default listing derives no archivedAt for any content version.
    hidden.forEach(v -> assertNull(v.getArchivedAt()));

    // Current version is never superseded, even in the opt-in view.
    assertNull(shown.get(0).getArchivedAt());

    BlobMetadata markerEntry = shown.get(1);
    assertTrue(markerEntry.isArchived());

    // In the opt-in view the older version's archivedAt is the marker's creation time (the instant
    // it stopped being current), not version B's creation time.
    BlobMetadata oldestShown = shown.get(2);
    assertFalse(oldestShown.isArchived());
    assertNotNull(oldestShown.getArchivedAt());
    assertEquals(markerEntry.getCreatedTime(), oldestShown.getArchivedAt());
    // Validity interval is end-exclusive: version A's window ends exactly at the marker instant.
    assertTrue(oldestShown.getCreatedTime().isBefore(oldestShown.getArchivedAt()));
  }

  @Test
  void deleteSpecificVersion_removesMarkerHistoryForThatVersion() {
    String key = "obj";
    upload(key, "A");
    store.delete(key, null); // create a marker

    // The marker is visible before it is deleted by version id.
    List<BlobMetadata> before = list(key, true);
    String markerVersionId =
        before.stream()
            .filter(BlobMetadata::isArchived)
            .map(BlobMetadata::getVersionId)
            .findFirst()
            .orElseThrow();

    store.delete(key, markerVersionId);

    List<BlobMetadata> after = list(key, true);
    after.forEach(v -> assertFalse(v.isArchived()));
  }

  @Test
  void exactKeyGuard_doesNotLeakSiblingKeys() {
    // "obj:child" shares the "obj" storage prefix; the remainder past the prefix still contains a
    // ':' so it must be excluded from an exact-key listing of "obj".
    upload("obj", "A");
    upload("obj:child", "child-A");

    List<BlobMetadata> versions = list("obj", true);

    assertEquals(1, versions.size());
    assertEquals("obj", versions.get(0).getKey());
  }
}
