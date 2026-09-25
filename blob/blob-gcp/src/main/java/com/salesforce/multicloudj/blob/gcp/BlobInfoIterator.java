package com.salesforce.multicloudj.blob.gcp;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.salesforce.multicloudj.blob.driver.BlobInfo;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/** Lazily iterates GCS list pages while preserving deterministic ordering within each page. */
final class BlobInfoIterator implements Iterator<BlobInfo> {

  private final boolean includeCommonPrefixes;
  private Page<Blob> currentPage;
  private Iterator<BlobInfo> currentBatch;

  BlobInfoIterator(Page<Blob> firstPage, boolean includeCommonPrefixes) {
    this.currentPage = firstPage;
    this.includeCommonPrefixes = includeCommonPrefixes;
    this.currentBatch = toSortedBatch(firstPage).iterator();
  }

  private List<BlobInfo> toSortedBatch(Page<Blob> page) {
    List<BlobInfo> entries = new ArrayList<>();
    for (Blob blob : page.getValues()) {
      if (!includeCommonPrefixes && blob.isDirectory()) {
        continue;
      }
      entries.add(
          BlobInfo.builder()
              .withKey(blob.getName())
              .withObjectSize(blob.getSize())
              .withLastModified(
                  blob.getUpdateTimeOffsetDateTime() != null
                      ? blob.getUpdateTimeOffsetDateTime().toInstant()
                      : null)
              .withCommonPrefix(blob.isDirectory())
              .build());
    }
    entries.sort(Comparator.comparing(BlobInfo::getKey).thenComparing(BlobInfo::isCommonPrefix));
    return entries;
  }

  @Override
  public boolean hasNext() {
    while (!currentBatch.hasNext() && currentPage.hasNextPage()) {
      currentPage = currentPage.getNextPage();
      currentBatch = toSortedBatch(currentPage).iterator();
    }
    return currentBatch.hasNext();
  }

  @Override
  public BlobInfo next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return currentBatch.next();
  }
}
