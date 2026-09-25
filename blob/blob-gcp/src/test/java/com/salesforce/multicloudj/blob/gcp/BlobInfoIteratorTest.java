package com.salesforce.multicloudj.blob.gcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.salesforce.multicloudj.blob.driver.BlobInfo;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class BlobInfoIteratorTest {

  @Test
  void sortsCommonPrefixesAndObjectsByKeyWithinPage() {
    Blob object = blob("b.txt", false);
    Blob commonPrefix = blob("a/", true);
    Page<Blob> page = mock(Page.class);
    when(page.getValues()).thenReturn(List.of(object, commonPrefix));

    BlobInfoIterator iterator = new BlobInfoIterator(page, true);
    List<BlobInfo> entries = new ArrayList<>();
    iterator.forEachRemaining(entries::add);

    assertEquals(List.of("a/", "b.txt"), entries.stream().map(BlobInfo::getKey).toList());
    assertTrue(entries.get(0).isCommonPrefix());
    assertFalse(entries.get(1).isCommonPrefix());
  }

  @Test
  void loadsLaterPagesLazilyAndSkipsEmptyIntermediatePage() {
    Page<Blob> firstPage = mock(Page.class);
    Page<Blob> emptyIntermediatePage = mock(Page.class);
    Page<Blob> lastPage = mock(Page.class);
    Blob firstBlob = blob("first.txt", false);
    Blob commonPrefix = blob("folder/", true);
    Blob lastBlob = blob("last.txt", false);
    when(firstPage.getValues()).thenReturn(List.of(firstBlob));
    when(firstPage.hasNextPage()).thenReturn(true);
    when(firstPage.getNextPage()).thenReturn(emptyIntermediatePage);
    when(emptyIntermediatePage.getValues()).thenReturn(List.of(commonPrefix));
    when(emptyIntermediatePage.hasNextPage()).thenReturn(true);
    when(emptyIntermediatePage.getNextPage()).thenReturn(lastPage);
    when(lastPage.getValues()).thenReturn(List.of(lastBlob));

    BlobInfoIterator iterator = new BlobInfoIterator(firstPage, false);
    verify(firstPage, never()).getNextPage();
    assertEquals("first.txt", iterator.next().getKey());
    verify(firstPage, never()).getNextPage();

    assertTrue(iterator.hasNext());
    assertEquals("last.txt", iterator.next().getKey());
    assertFalse(iterator.hasNext());
    verify(firstPage).getNextPage();
    verify(emptyIntermediatePage).getNextPage();
  }

  private Blob blob(String key, boolean commonPrefix) {
    Blob blob = mock(Blob.class);
    when(blob.getName()).thenReturn(key);
    when(blob.isDirectory()).thenReturn(commonPrefix);
    return blob;
  }
}
