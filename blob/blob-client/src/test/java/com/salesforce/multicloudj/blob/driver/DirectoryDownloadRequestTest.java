package com.salesforce.multicloudj.blob.driver;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

public class DirectoryDownloadRequestTest {

  @Test
  void isExcludedMatchesFullKeyPrefix() {
    DirectoryDownloadRequest request =
        DirectoryDownloadRequest.builder()
            .prefixToDownload("photos/")
            .prefixesToExclude(List.of("photos/tmp/", "photos/cache"))
            .build();

    assertTrue(request.isExcluded("photos/tmp/a.jpg"));
    assertTrue(request.isExcluded("photos/cache/b.jpg"));
    assertTrue(request.isExcluded("photos/cacheOld/c.jpg"));
    assertFalse(request.isExcluded("photos/keep/d.jpg"));
    // Matching is against the full key, not the key relative to prefixToDownload.
    assertFalse(request.isExcluded("tmp/e.jpg"));
  }

  @Test
  void isExcludedReturnsFalseForNullOrEmptyList() {
    DirectoryDownloadRequest nullList = DirectoryDownloadRequest.builder().build();
    DirectoryDownloadRequest emptyList =
        DirectoryDownloadRequest.builder().prefixesToExclude(List.of()).build();

    assertFalse(nullList.isExcluded("photos/a.jpg"));
    assertFalse(emptyList.isExcluded("photos/a.jpg"));
  }
}
