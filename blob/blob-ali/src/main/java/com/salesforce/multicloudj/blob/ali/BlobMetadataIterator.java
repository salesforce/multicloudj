package com.salesforce.multicloudj.blob.ali;

import com.aliyun.sdk.service.oss2.OSSClient;
import com.aliyun.sdk.service.oss2.models.DeleteMarkerEntry;
import com.aliyun.sdk.service.oss2.models.ListObjectVersionsRequest;
import com.aliyun.sdk.service.oss2.models.ListObjectVersionsResult;
import com.aliyun.sdk.service.oss2.models.ObjectVersion;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Iterator that retrieves {@link BlobMetadata} versions for an exact key.
 *
 * <p>Iteration is lazy and streaming: OSS pages are pulled on demand and only one bounded page is
 * buffered at a time. OSS returns content versions and delete markers in two separate per-page
 * collections, and pages themselves arrive newest-first. Because every page holds the next slice of
 * a single global newest-first sequence for the key, the two per-page collections can be merged
 * within each page (a two-pointer merge) and the pages consumed in order to reproduce the true
 * newest-first timeline without ever sorting the complete remote result or reaching across a page
 * boundary. The paginator propagates the OSS key and version-id continuation markers, so pagination
 * stays native. OSS returns each collection newest-first, which the per-page two-pointer merge
 * relies on.
 *
 * <p>Both collections always participate in the ordering so supersession times stay correct: a
 * content version stops being current when the next entry is created, and that superseding entry
 * may be a delete marker. Each emitted entry's {@code archivedAt} is the creation time of the
 * entry immediately newer than it; the newest entry is still current and therefore has none. OSS
 * does not report a per-version supersession timestamp, so it is derived from the successor entry
 * on this timeline. Delete markers are only emitted when {@code includeDeleteMarkers} is set, but a
 * hidden marker still advances the supersession pointer so the version below it reports the correct
 * {@code archivedAt}.
 *
 * <p>When a content version and a delete marker carry the same {@code lastModified} (a same-instant
 * PUT then DELETE), timestamps alone cannot order them. OSS flags exactly one entry as the latest
 * version, so that flag breaks the tie: the entry OSS considers current is emitted first and
 * therefore reports no {@code archivedAt}. Same-instant ties deeper in the history, where neither
 * entry is flagged latest, keep a stable content-version-first ordering.
 */
public class BlobMetadataIterator implements Iterator<BlobMetadata> {

  private final String key;
  private final boolean includeDeleteMarkers;
  private final Iterator<ListObjectVersionsResult> responseIterator;

  // Exact-key-filtered, newest-first collections of the current page only, with merge cursors.
  private List<ObjectVersion> pageVersions = Collections.emptyList();
  private List<DeleteMarkerEntry> pageMarkers = Collections.emptyList();
  private int versionCursor;
  private int markerCursor;

  // lastModified of the immediately-newer timeline entry (null for the newest entry overall).
  private Instant previousLastModified;

  // One-element lookahead so filtered-out markers can be skipped transparently.
  private BlobMetadata nextEmit;
  private boolean exhausted;

  public BlobMetadataIterator(OSSClient ossClient, String bucket, String key) {
    this(ossClient, bucket, key, false);
  }

  public BlobMetadataIterator(
      OSSClient ossClient, String bucket, String key, boolean includeDeleteMarkers) {
    this.key = key;
    this.includeDeleteMarkers = includeDeleteMarkers;
    this.responseIterator =
        ossClient
            .listObjectVersionsPaginator(
                ListObjectVersionsRequest.newBuilder().bucket(bucket).prefix(key).build())
            .iterator();
  }

  /**
   * Internal holder that unifies content versions and delete markers so both sit on a single
   * timeline before the cloud-neutral {@link BlobMetadata} is produced.
   */
  private static final class Entry {
    private final Instant lastModified;
    private final boolean deleteMarker;
    private final ObjectVersion version; // non-null when deleteMarker == false
    private final DeleteMarkerEntry markerEntry; // non-null when deleteMarker == true

    private Entry(ObjectVersion version) {
      this.version = version;
      this.markerEntry = null;
      this.deleteMarker = false;
      this.lastModified = version.lastModified();
    }

    private Entry(DeleteMarkerEntry markerEntry) {
      this.version = null;
      this.markerEntry = markerEntry;
      this.deleteMarker = true;
      this.lastModified = markerEntry.lastModified();
    }
  }

  /**
   * Advances the merged timeline until the next emittable entry is buffered or the stream ends.
   * Every entry consumed advances the supersession pointer, including hidden delete markers, so a
   * content version's {@code archivedAt} still reflects a superseding marker that is not emitted.
   */
  private void advance() {
    Entry entry;
    while ((entry = nextTimelineEntry()) != null) {
      Instant archivedAt = previousLastModified;
      previousLastModified = entry.lastModified;
      if (entry.deleteMarker) {
        if (!includeDeleteMarkers) {
          continue;
        }
        nextEmit = toMarkerMetadata(entry.markerEntry, archivedAt);
        return;
      }
      nextEmit = toVersionMetadata(entry.version, archivedAt);
      return;
    }
    exhausted = true;
  }

  /**
   * Returns the next entry in the global newest-first timeline, loading pages on demand, or null
   * when the stream is exhausted. Pages that hold no matching key after filtering (both cursors at
   * their end) simply cause the following page to be loaded.
   */
  private Entry nextTimelineEntry() {
    while (versionCursor >= pageVersions.size() && markerCursor >= pageMarkers.size()) {
      if (!loadNextPage()) {
        return null;
      }
    }
    boolean takeVersion;
    if (markerCursor >= pageMarkers.size()) {
      takeVersion = true;
    } else if (versionCursor >= pageVersions.size()) {
      takeVersion = false;
    } else {
      ObjectVersion version = pageVersions.get(versionCursor);
      DeleteMarkerEntry marker = pageMarkers.get(markerCursor);
      Instant versionTime = version.lastModified();
      Instant markerTime = marker.lastModified();
      if (versionTime != null && versionTime.equals(markerTime)) {
        // Same-instant PUT+DELETE: timestamps cannot order them, so honor OSS's latest flag. If
        // neither entry is flagged latest (a deeper same-instant tie), keep the content version
        // first for a stable outcome.
        takeVersion = !Boolean.TRUE.equals(marker.isLatest());
      } else {
        takeVersion = !isNewer(markerTime, versionTime);
      }
    }
    return takeVersion
        ? new Entry(pageVersions.get(versionCursor++))
        : new Entry(pageMarkers.get(markerCursor++));
  }

  /**
   * Loads the next page from the paginator and filters both collections down to the exact key,
   * resetting the merge cursors. Returns false when no more pages remain.
   */
  private boolean loadNextPage() {
    if (!responseIterator.hasNext()) {
      return false;
    }
    ListObjectVersionsResult page = responseIterator.next();
    List<ObjectVersion> versions = new ArrayList<>();
    if (page.versions() != null) {
      for (ObjectVersion version : page.versions()) {
        // The OSS prefix filter returns keys that START with the prefix, not exact matches.
        if (key.equals(version.key())) {
          versions.add(version);
        }
      }
    }
    List<DeleteMarkerEntry> markers = new ArrayList<>();
    if (page.deleteMarkers() != null) {
      for (DeleteMarkerEntry markerEntry : page.deleteMarkers()) {
        if (key.equals(markerEntry.key())) {
          markers.add(markerEntry);
        }
      }
    }
    // OSS returns each collection newest-first, which the per-page two-pointer merge relies on.
    pageVersions = versions;
    pageMarkers = markers;
    versionCursor = 0;
    markerCursor = 0;
    return true;
  }

  private static BlobMetadata toMarkerMetadata(DeleteMarkerEntry marker, Instant archivedAt) {
    return BlobMetadata.builder()
        .key(marker.key())
        .versionId(marker.versionId())
        .archived(true)
        .lastModified(marker.lastModified())
        .createdTime(marker.lastModified())
        .archivedAt(archivedAt)
        .build();
  }

  private static BlobMetadata toVersionMetadata(ObjectVersion version, Instant archivedAt) {
    return BlobMetadata.builder()
        .key(version.key())
        .versionId(version.versionId())
        .eTag(version.eTag())
        .objectSize(version.size() != null ? version.size() : 0L)
        .lastModified(version.lastModified())
        .createdTime(version.lastModified())
        .archivedAt(archivedAt)
        .build();
  }

  /** Returns true when {@code candidate} is strictly newer than {@code reference}. */
  private static boolean isNewer(Instant candidate, Instant reference) {
    if (candidate == null) {
      return false;
    }
    if (reference == null) {
      return true;
    }
    return candidate.isAfter(reference);
  }

  @Override
  public boolean hasNext() {
    if (nextEmit == null && !exhausted) {
      advance();
    }
    return nextEmit != null;
  }

  @Override
  public BlobMetadata next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    BlobMetadata result = nextEmit;
    nextEmit = null;
    return result;
  }
}
