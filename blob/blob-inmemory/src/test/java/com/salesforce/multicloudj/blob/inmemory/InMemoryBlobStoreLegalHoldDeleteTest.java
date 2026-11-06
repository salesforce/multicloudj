package com.salesforce.multicloudj.blob.inmemory;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
import com.salesforce.multicloudj.blob.driver.ListBlobVersionsRequest;
import com.salesforce.multicloudj.blob.driver.ObjectLockConfiguration;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.common.exceptions.BatchOperationException;
import com.salesforce.multicloudj.common.exceptions.FailedPreconditionException;
import com.salesforce.multicloudj.common.exceptions.ItemFailure;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for legal-hold enforcement on version-specific deletes. */
class InMemoryBlobStoreLegalHoldDeleteTest {

  private static final String BUCKET = "bucket-legal-hold";

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

  private String upload(String key, boolean legalHold) {
    byte[] bytes = "content".getBytes();
    UploadRequest.Builder request =
        new UploadRequest.Builder().withKey(key).withContentLength(bytes.length);
    if (legalHold) {
      request.withObjectLock(ObjectLockConfiguration.builder().legalHold(true).build());
    }
    return store.upload(request.build(), new ByteArrayInputStream(bytes)).getVersionId();
  }

  private boolean exists(String key, String versionId) {
    try {
      store.getMetadata(key, versionId);
      return true;
    } catch (ResourceNotFoundException e) {
      return false;
    }
  }

  @Test
  void singleDelete_heldVersion_throwsFailedPreconditionAndKeepsVersion() {
    String version = upload("held", true);

    assertThrows(FailedPreconditionException.class, () -> store.delete("held", version));

    assertTrue(exists("held", version));
    assertTrue(store.getObjectLock("held", version).isLegalHold());
  }

  @Test
  void singleDelete_unheldVersion_removesVersion() {
    String version = upload("free", false);

    store.delete("free", version);

    assertFalse(exists("free", version));
  }

  @Test
  void singleDelete_unheldVersionOfKeyWithHeldVersion_removesOnlyThatVersion() {
    String held = upload("key", true);
    String free = upload("key", false);

    store.delete("key", free);

    assertFalse(exists("key", free));
    assertTrue(exists("key", held));
  }

  @Test
  void unversionedDelete_heldKey_addsMarkerAndKeepsHeldVersion() {
    String version = upload("held", true);

    assertDoesNotThrow(() -> store.delete("held", null));

    assertTrue(exists("held", version));
    assertTrue(store.getObjectLock("held", version).isLegalHold());
    List<Object> entries = new ArrayList<>();
    store
        .listBlobVersions(
            ListBlobVersionsRequest.builder().withKey("held").withIncludeArchived(true).build())
        .forEachRemaining(entries::add);
    assertEquals(2, entries.size());
  }

  @Test
  void singleDelete_absentVersion_isSilentSuccess() {
    String version = upload("gone", false);
    store.delete("gone", version);

    assertDoesNotThrow(() -> store.delete("gone", version));
  }

  @Test
  void singleDelete_afterHoldReleased_succeeds() {
    String version = upload("held", true);
    assertThrows(FailedPreconditionException.class, () -> store.delete("held", version));

    store.updateLegalHold("held", version, false);
    store.delete("held", version);

    assertFalse(exists("held", version));
  }

  @Test
  void updateLegalHold_isStoredPerVersion() {
    String first = upload("key", false);
    String second = upload("key", false);

    store.updateLegalHold("key", first, true);

    assertTrue(store.getObjectLock("key", first).isLegalHold());
    assertTrue(
        store.getObjectLock("key", second) == null
            || !store.getObjectLock("key", second).isLegalHold());
    assertThrows(FailedPreconditionException.class, () -> store.delete("key", first));
    store.delete("key", second);
    assertFalse(exists("key", second));
    assertTrue(exists("key", first));
  }

  @Test
  void batchDelete_freeHeldAndAbsent_reportsOnlyHeldAndDeletesFree() {
    String freeVersion = upload("free", false);
    String heldVersion = upload("held", true);
    String absentVersion = upload("absent", false);
    store.delete("absent", absentVersion);

    BatchOperationException e =
        assertThrows(
            BatchOperationException.class,
            () ->
                store.delete(
                    List.of(
                        new BlobIdentifier("free", freeVersion),
                        new BlobIdentifier("held", heldVersion),
                        new BlobIdentifier("absent", absentVersion))));

    assertEquals(1, e.getFailures().size());
    assertEquals(List.of("held"), e.getFailedItemIds());
    ItemFailure failure = e.getFailures().get(0);
    assertEquals(heldVersion, failure.getAttributes().get(BlobIdentifier.VERSION_ID_ATTRIBUTE));
    assertInstanceOf(FailedPreconditionException.class, failure.getException());
    assertFalse(failure.isRetryable());
    assertFalse(exists("free", freeVersion));
    assertTrue(exists("held", heldVersion));
    assertFalse(exists("absent", absentVersion));
  }

  @Test
  void batchDelete_processesItemsAfterHeldFailure() {
    String heldVersion = upload("held", true);
    String laterVersion = upload("later", false);

    assertThrows(
        BatchOperationException.class,
        () ->
            store.delete(
                List.of(
                    new BlobIdentifier("held", heldVersion),
                    new BlobIdentifier("later", laterVersion))));

    assertFalse(exists("later", laterVersion));
    assertTrue(exists("held", heldVersion));
  }

  @Test
  void batchDelete_duplicatedHeldIdentifier_reportsSingleFailure() {
    String heldVersion = upload("held", true);

    BatchOperationException e =
        assertThrows(
            BatchOperationException.class,
            () ->
                store.delete(
                    List.of(
                        new BlobIdentifier("held", heldVersion),
                        new BlobIdentifier("held", heldVersion))));

    assertEquals(1, e.getFailures().size());
    assertTrue(exists("held", heldVersion));
  }

  @Test
  void batchDelete_heldVersionAfterRelease_succeeds() {
    String heldVersion = upload("held", true);
    store.updateLegalHold("held", heldVersion, false);

    assertDoesNotThrow(() -> store.delete(List.of(new BlobIdentifier("held", heldVersion))));

    assertFalse(exists("held", heldVersion));
  }

  @Test
  void batchDelete_allFree_throwsNothing() {
    String first = upload("one", false);
    String second = upload("two", false);

    assertDoesNotThrow(
        () ->
            store.delete(
                List.of(new BlobIdentifier("one", first), new BlobIdentifier("two", second))));

    assertFalse(exists("one", first));
    assertFalse(exists("two", second));
  }

  @Test
  void batchDelete_unversionedIdentifierOfHeldKey_doesNotFail() {
    String heldVersion = upload("held", true);

    assertDoesNotThrow(() -> store.delete(List.of(new BlobIdentifier("held", null))));

    assertNotNull(store.getObjectLock("held", heldVersion));
    assertTrue(exists("held", heldVersion));
  }

  @Test
  void batchDelete_nullVersionAndLiteralNullVersion_areProcessedIndependently() {
    String heldVersion = upload("held", true);

    assertDoesNotThrow(
        () ->
            store.delete(
                List.of(new BlobIdentifier("held", null), new BlobIdentifier("held", "null"))));

    assertTrue(exists("held", heldVersion));
    assertTrue(store.getObjectLock("held", heldVersion).isLegalHold());
    List<Object> entries = new ArrayList<>();
    store
        .listBlobVersions(
            ListBlobVersionsRequest.builder().withKey("held").withIncludeArchived(true).build())
        .forEachRemaining(entries::add);
    assertEquals(2, entries.size());
  }
}
