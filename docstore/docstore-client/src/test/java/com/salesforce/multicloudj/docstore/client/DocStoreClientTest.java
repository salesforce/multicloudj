package com.salesforce.multicloudj.docstore.client;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.docstore.driver.AbstractDocStore;
import com.salesforce.multicloudj.docstore.driver.ActionList;
import com.salesforce.multicloudj.docstore.driver.CollectionOptions;
import com.salesforce.multicloudj.docstore.driver.Document;
import java.net.URI;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DocStoreClientTest {

  static class MockDocStoreClient extends DocStoreClient {
    public MockDocStoreClient(AbstractDocStore docStore) {
      super(docStore);
    }
  }

  AbstractDocStore mockDocStore = mock(AbstractDocStore.class);
  MockDocStoreClient mockClient = new MockDocStoreClient(mockDocStore);

  @Test
  void testCreateDocStoreClient() {
    String provider = "aws"; // or "ali"

    CollectionOptions collectionOptions =
        new CollectionOptions.CollectionOptionsBuilder()
            .withTableName("qzhouDS")
            .withPartitionKey("title")
            .withSortKey("publisher")
            .withAllowScans(false)
            .withRevisionField("docRevision")
            .build();

    DocStoreClient.DocStoreClientBuilder builder = mock(DocStoreClient.DocStoreClientBuilder.class);
    when(builder.withRegion(anyString())).thenReturn(builder);
    when(builder.withEndpoint(any())).thenReturn(builder);
    when(builder.withCollectionOptions(any())).thenReturn(builder);

    Assertions.assertDoesNotThrow(
        () ->
            builder
                .withRegion("us-west-2")
                .withEndpoint(URI.create("https://your-url"))
                .withCollectionOptions(collectionOptions)
                .build());
  }

  @Test
  void testCreate() {
    ActionList actionList = new ActionList(mockDocStore);
    Document doc = mock(Document.class);
    when(mockDocStore.getActions()).thenReturn(actionList);
    mockClient.create(doc);
    Assertions.assertEquals(1, actionList.getActions().size());
    mockClient.close();
  }

  @Test
  void testCreateReal() {
    ActionList actionList = new ActionList(mockDocStore);
    Document doc = mock(Document.class);
    when(mockDocStore.getActions()).thenReturn(actionList);
    mockClient.create(doc);
    Assertions.assertEquals(1, actionList.getActions().size());
    mockClient.close();
  }

  @Test
  void testReplace() {
    ActionList actionList = new ActionList(mockDocStore);
    Document doc = mock(Document.class);
    when(mockDocStore.getActions()).thenReturn(actionList);
    mockClient.replace(doc);
    mockClient.close();
  }

  @Test
  void testPut() {
    ActionList actionList = new ActionList(mockDocStore);
    Document doc = mock(Document.class);
    when(mockDocStore.getActions()).thenReturn(actionList);
    mockClient.put(doc);
    mockClient.close();
  }

  @Test
  void testDelete() {
    ActionList actionList = new ActionList(mockDocStore);
    Document doc = mock(Document.class);
    when(mockDocStore.getActions()).thenReturn(actionList);
    mockClient.delete(doc);
    mockClient.close();
  }

  @Test
  void testGet() {
    Document doc = mock(Document.class);
    mockClient.get(doc, "path");
    verify(mockDocStore).batchGet(List.of(doc), false, "path");
  }

  @Test
  void testGetWithConsistentRead() {
    Document doc = mock(Document.class);
    mockClient.get(doc, true, "path");
    verify(mockDocStore).batchGet(List.of(doc), true, "path");
  }

  @Test
  void testUpdate() {
    ActionList actionList = new ActionList(mockDocStore);
    Document doc = mock(Document.class);
    when(mockDocStore.getActions()).thenReturn(actionList);
    Assertions.assertThrows(
        UnSupportedOperationException.class, () -> mockClient.update(doc, null));
    mockClient.close();
  }

  @Test
  void testBatchGet() {
    Document doc = mock(Document.class);
    mockClient.batchGet(List.of(doc));
    verify(mockDocStore).batchGet(List.of(doc), false);
    mockClient.close();
  }

  @Test
  void testBatchGetWithConsistentRead() {
    List<Document> docs = List.of(mock(Document.class), mock(Document.class));
    mockClient.batchGet(docs, true);
    verify(mockDocStore).batchGet(docs, true);
  }

  @Test
  void testReadFailuresAreMappedAtClientBoundary() {
    Document doc = mock(Document.class);
    RuntimeException nativeFailure = new IllegalArgumentException("invalid key");
    InvalidArgumentException mapped = new InvalidArgumentException("invalid key");
    doThrow(nativeFailure).when(mockDocStore).batchGet(List.of(doc), true);
    when(mockDocStore.mapException(nativeFailure)).thenReturn(mapped);
    Assertions.assertSame(
        mapped,
        Assertions.assertThrows(InvalidArgumentException.class, () -> mockClient.get(doc, true)));
    Assertions.assertSame(
        mapped,
        Assertions.assertThrows(
            InvalidArgumentException.class, () -> mockClient.batchGet(List.of(doc), true)));
  }

  @Test
  void testBatchPut() {
    ActionList actionList = new ActionList(mockDocStore);
    Document doc = mock(Document.class);
    when(mockDocStore.getActions()).thenReturn(actionList);
    mockClient.batchPut(List.of(doc));
    mockClient.close();
  }

  @Test
  void testRunActions() {
    ActionList actionList = new ActionList(mockDocStore);
    when(mockClient.getActions()).thenReturn(actionList);
    Assertions.assertEquals(actionList, mockClient.getActions());
  }
}
