package com.salesforce.multicloudj.docstore.client;

import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.docstore.driver.AbstractDocStore;
import com.salesforce.multicloudj.docstore.driver.ActionList;
import com.salesforce.multicloudj.docstore.driver.CollectionOptions;
import com.salesforce.multicloudj.docstore.driver.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

        CollectionOptions collectionOptions = new CollectionOptions.CollectionOptionsBuilder()
                .withTableName("qzhouDS")
                .withPartitionKey("title")
                .withSortKey("publisher")
                .withAllowScans(false)
                .withRevisionField("docRevision")
                .build();

        DocStoreClient.DocStoreClientBuilder builder  = mock(DocStoreClient.DocStoreClientBuilder.class);
        when(builder.withRegion(anyString())).thenReturn(builder);
        when(builder.withEndpoint(any())).thenReturn(builder);
        when(builder.withCollectionOptions(any())).thenReturn(builder);

        Assertions.assertDoesNotThrow(()-> builder.withRegion("us-west-2")
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
        ActionList actionList = new ActionList(mockDocStore);
        Document doc = mock(Document.class);
        when(mockDocStore.getActions()).thenReturn(actionList);
        mockClient.get(doc, "path");
        Assertions.assertEquals(1, actionList.getActions().size());
    }

    @Test
    void testUpdate() {
        ActionList actionList = new ActionList(mockDocStore);
        Document doc = mock(Document.class);
        when(mockDocStore.getActions()).thenReturn(actionList);
        Assertions.assertThrows(UnSupportedOperationException.class, () -> mockClient.update(doc, null));
        mockClient.close();
    }

    @Test
    void testBatchGet() {
        ActionList actionList = new ActionList(mockDocStore);
        Document doc = mock(Document.class);
        when(mockDocStore.getActions()).thenReturn(actionList);
        mockClient.batchGet(List.of(doc));
        mockClient.close();
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
