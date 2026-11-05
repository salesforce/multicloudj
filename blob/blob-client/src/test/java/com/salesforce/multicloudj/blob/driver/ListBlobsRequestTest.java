package com.salesforce.multicloudj.blob.driver;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Unit tests for the synchronous blob-list request. */
public class ListBlobsRequestTest {

  @Test
  void includeCommonPrefixesDefaultsToFalse() {
    ListBlobsRequest request = ListBlobsRequest.builder().withDelimiter("/").build();

    assertFalse(request.isIncludeCommonPrefixes());
  }

  @Test
  void includeCommonPrefixesCanBeEnabled() {
    ListBlobsRequest request =
        ListBlobsRequest.builder().withDelimiter("/").withIncludeCommonPrefixes(true).build();

    assertTrue(request.isIncludeCommonPrefixes());
  }
}
