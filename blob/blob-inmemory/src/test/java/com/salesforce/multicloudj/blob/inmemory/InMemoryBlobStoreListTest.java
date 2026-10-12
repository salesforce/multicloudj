package com.salesforce.multicloudj.blob.inmemory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.salesforce.multicloudj.blob.driver.BlobInfo;
import com.salesforce.multicloudj.blob.driver.ListBlobsRequest;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class InMemoryBlobStoreListTest {

  private static final String BUCKET = "list-bucket";
  private InMemoryBlobStore store;

  @BeforeEach
  void setUp() {
    InMemoryBlobStore.clearStorage();
    InMemoryBlobStore.createBucket(BUCKET);
    store = new InMemoryBlobStore.Builder().withBucket(BUCKET).withRegion("local").build();
  }

  @AfterEach
  void tearDown() {
    InMemoryBlobStore.clearStorage();
  }

  @Test
  void listRepresentsMarkerCollisionOnlyAsCommonPrefix() {
    upload("base/directory/");
    upload("base/directory/blob.txt");

    Iterator<BlobInfo> iterator =
        store.list(
            ListBlobsRequest.builder()
                .withPrefix("base/")
                .withDelimiter("/")
                .withIncludeCommonPrefixes(true)
                .build());
    List<BlobInfo> entries = new ArrayList<>();
    iterator.forEachRemaining(entries::add);

    assertEquals(1, entries.size());
    assertEquals("base/directory/", entries.get(0).getKey());
    assertTrue(entries.get(0).isCommonPrefix());
  }

  private void upload(String key) {
    byte[] content = "test".getBytes(StandardCharsets.UTF_8);
    store.upload(
        UploadRequest.builder().withKey(key).withContentLength(content.length).build(), content);
  }
}
