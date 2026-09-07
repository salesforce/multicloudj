package com.salesforce.multicloudj.blob.aws;

import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteMarkerEntry;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsResponse;
import software.amazon.awssdk.services.s3.model.ObjectVersion;

/** Iterator object to retrieve BlobMetadata versions for an exact key. */
public class BlobMetadataIterator implements Iterator<BlobMetadata> {

  private final String key;
  private final boolean includeDeleteMarkers;
  private final Iterator<ListObjectVersionsResponse> responseIterator;
  private Iterator<BlobMetadata> resolved;

  public BlobMetadataIterator(S3Client s3Client, String bucket, String key) {
    this(s3Client, bucket, key, false);
  }

  public BlobMetadataIterator(
      S3Client s3Client, String bucket, String key, boolean includeDeleteMarkers) {
    this.key = key;
    this.includeDeleteMarkers = includeDeleteMarkers;
    this.responseIterator =
        s3Client
            .listObjectVersionsPaginator(
                ListObjectVersionsRequest.builder().bucket(bucket).prefix(key).build())
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
   * Drains every page for the exact key and resolves the combined timeline. S3 returns content
   * versions and delete markers in two separate per-page collections, each already ordered
   * newest-first. Both are always gathered so that supersession times are correct: a content
   * version stops being current when the next entry is created, and that superseding entry may be a
   * delete marker. The two newest-first lists are merged into one newest-first timeline; each
   * entry's supersession time is the creation time of the entry immediately newer than it, and the
   * newest entry is still current and therefore has none. Delete markers are only emitted when
   * {@code includeDeleteMarkers} is set, but they always participate in the ordering.
   */
  private void resolve() {
    List<ObjectVersion> versions = new ArrayList<>();
    List<DeleteMarkerEntry> markers = new ArrayList<>();
    while (responseIterator.hasNext()) {
      ListObjectVersionsResponse page = responseIterator.next();
      for (ObjectVersion version : page.versions()) {
        // S3's prefix filter returns keys that START with the prefix, not exact matches.
        if (key.equals(version.key())) {
          versions.add(version);
        }
      }
      for (DeleteMarkerEntry markerEntry : page.deleteMarkers()) {
        if (key.equals(markerEntry.key())) {
          markers.add(markerEntry);
        }
      }
    }

    // Merge the two newest-first lists into one newest-first timeline. When there are no delete
    // markers this preserves the version order S3 returned.
    List<Entry> timeline = new ArrayList<>(versions.size() + markers.size());
    int vi = 0;
    int mi = 0;
    while (vi < versions.size() || mi < markers.size()) {
      boolean takeVersion;
      if (mi >= markers.size()) {
        takeVersion = true;
      } else if (vi >= versions.size()) {
        takeVersion = false;
      } else {
        takeVersion = !isNewer(markers.get(mi).lastModified(), versions.get(vi).lastModified());
      }
      timeline.add(takeVersion ? new Entry(versions.get(vi++)) : new Entry(markers.get(mi++)));
    }

    List<BlobMetadata> result = new ArrayList<>(timeline.size());
    for (int i = 0; i < timeline.size(); i++) {
      Entry entry = timeline.get(i);
      Instant noncurrentAt = (i == 0) ? null : timeline.get(i - 1).lastModified;
      if (entry.deleteMarker) {
        if (!includeDeleteMarkers) {
          continue;
        }
        DeleteMarkerEntry marker = entry.markerEntry;
        result.add(
            BlobMetadata.builder()
                .key(marker.key())
                .versionId(marker.versionId())
                .deleteMarker(true)
                .lastModified(marker.lastModified())
                .createdTime(marker.lastModified())
                .noncurrentAt(noncurrentAt)
                .build());
      } else {
        ObjectVersion version = entry.version;
        result.add(
            BlobMetadata.builder()
                .key(version.key())
                .versionId(version.versionId())
                .eTag(version.eTag())
                .objectSize(version.size())
                .lastModified(version.lastModified())
                .createdTime(version.lastModified())
                .noncurrentAt(noncurrentAt)
                .build());
      }
    }
    resolved = result.iterator();
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
    if (resolved == null) {
      resolve();
    }
    return resolved.hasNext();
  }

  @Override
  public BlobMetadata next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return resolved.next();
  }
}
