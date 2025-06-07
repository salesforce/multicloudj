package com.salesforce.multicloudj.docstore.gcp;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.ServerStream;
import com.google.api.gax.rpc.ServerStreamingCallable;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.firestore.v1.FirestoreClient;
import com.google.firestore.v1.CommitRequest;
import com.google.firestore.v1.CommitResponse;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.Write;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceAlreadyExistsException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.docstore.client.Query;
import com.salesforce.multicloudj.docstore.driver.Action;
import com.salesforce.multicloudj.docstore.driver.ActionKind;
import com.salesforce.multicloudj.docstore.driver.CollectionOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FSDocStoreTest {
    static class TestAction extends Action {
        public TestAction(ActionKind kind, com.salesforce.multicloudj.docstore.driver.Document document,
                          List<String> fieldPaths, Map<String, Object> mods, boolean inAtomicWrites) {
            super(kind, document, fieldPaths, mods, inAtomicWrites);
        }

        public TestAction(ActionKind kind, com.salesforce.multicloudj.docstore.driver.Document document,
                          List<String> fieldPaths, Map<String, Object> mods) {
            this(kind, document, fieldPaths, mods, false);
        }

        public void setKey(Object key) {
            this.key = key;
        }

        public void setIndex(int index) {
            this.index = index;
        }
    }

    private FSDocStore docStore;
    private FirestoreClient mockFirestoreClient;
    private CollectionOptions collectionOptions;

    @BeforeEach
    void setUp() {
        collectionOptions = new CollectionOptions.CollectionOptionsBuilder()
                .withTableName("projects/testDB/databases/(default)/documents/test-collection")
                .withPartitionKey("title")
                .withSortKey("publisher")
                .withAllowScans(true)
                .withRevisionField("docRevision")
                .build();

        mockFirestoreClient = mock(FirestoreClient.class);
        
        // Mock the commit response
        CommitResponse mockCommitResponse = CommitResponse.newBuilder().build();
        when(mockFirestoreClient.commit(any(CommitRequest.class))).thenReturn(mockCommitResponse);

        docStore = new FSDocStore.Builder()
                .withFirestoreV1Client(mockFirestoreClient)
                .withCollectionOptions(collectionOptions)
                .build();
    }

    @AfterEach
    void tearDown() {
        if (docStore != null) {
            docStore.close();
        }
    }

    @Test
    void testDefaultConstructor() {
        FSDocStore docStore = new FSDocStore();
        Assertions.assertNotNull(docStore);

        FSDocStore.Builder builder = docStore.builder();
        Assertions.assertNotNull(builder);
    }

    @Test
    void testProviderId() {
        Assertions.assertEquals("gcp-firestore", docStore.getProviderId());
    }

    @Test
    void testGetException() {
        // Test handling of SDK exceptions
        Class<?> clazz = docStore.getException(new InvalidArgumentException("Test"));
        Assertions.assertEquals(InvalidArgumentException.class, clazz);

        // Test handling of IllegalArgumentException
        clazz = docStore.getException(new IllegalArgumentException("Test"));
        Assertions.assertEquals(InvalidArgumentException.class, clazz);

        // Test handling of ApiException with INVALID_ARGUMENT status
        ApiException apiException = mock(ApiException.class);
        StatusCode statusCode = mock(StatusCode.class);
        when(statusCode.getCode()).thenReturn(StatusCode.Code.INVALID_ARGUMENT);
        when(apiException.getStatusCode()).thenReturn(statusCode);
        clazz = docStore.getException(apiException);
        Assertions.assertEquals(InvalidArgumentException.class, clazz);

        // Test handling of ApiException with NOT_FOUND status
        reset(statusCode);
        when(statusCode.getCode()).thenReturn(StatusCode.Code.NOT_FOUND);
        clazz = docStore.getException(apiException);
        Assertions.assertEquals(ResourceNotFoundException.class, clazz);

        // Test handling of ApiException with ALREADY_EXISTS status
        reset(statusCode);
        when(statusCode.getCode()).thenReturn(StatusCode.Code.ALREADY_EXISTS);
        clazz = docStore.getException(apiException);
        Assertions.assertEquals(ResourceAlreadyExistsException.class, clazz);

        // Test handling of unknown exceptions
        clazz = docStore.getException(new RuntimeException("Test"));
        Assertions.assertEquals(UnknownException.class, clazz);
    }

    @Test
    void testGetKey() throws Exception {
        // Test getting key from document with valid partition key
        com.salesforce.multicloudj.docstore.driver.Document doc = 
                new com.salesforce.multicloudj.docstore.driver.Document(
                        Map.of("title", "TestTitle", "publisher", "TestPublisher"));
        
        Object key = docStore.getKey(doc);
        Assertions.assertNotNull(key);
        
        // Since Key is private, use reflection to check the documentId
        Field documentIdField = key.getClass().getDeclaredField("documentId");
        documentIdField.setAccessible(true);
        Assertions.assertEquals("TestTitle:TestPublisher", documentIdField.get(key));
        
        // Test that exception is thrown when partition key is missing
        com.salesforce.multicloudj.docstore.driver.Document docWithoutKey = 
                new com.salesforce.multicloudj.docstore.driver.Document(
                        Map.of("publisher", "TestPublisher"));
        
        Assertions.assertThrows(IllegalArgumentException.class, () -> docStore.getKey(docWithoutKey));
    }

    @Test
    void testRunActions() throws Exception {
        // Create a document for our test
        Map<String, Object> docMap = new HashMap<>();
        docMap.put("title", "TestDoc");
        docMap.put("publisher", "TestPublisher");
        docMap.put("content", "This is test content");
        com.salesforce.multicloudj.docstore.driver.Document testDoc = 
                new com.salesforce.multicloudj.docstore.driver.Document(docMap);
        
        // Create a PUT action
        List<String> fieldPaths = null;
        Map<String, Object> mods = null;
        Action putAction = new TestAction(ActionKind.ACTION_KIND_PUT, testDoc, fieldPaths, mods);
        
        // Run the action
        List<Action> actions = new ArrayList<>();
        actions.add(putAction);
        docStore.runActions(actions, null);
        
        // Verify the commit was called
        ArgumentCaptor<CommitRequest> requestCaptor = ArgumentCaptor.forClass(CommitRequest.class);
        verify(mockFirestoreClient).commit(requestCaptor.capture());
        
        // Verify the request contents
        CommitRequest capturedRequest = requestCaptor.getValue();
        Assertions.assertNotNull(capturedRequest);
        
        // Verify database path
        Assertions.assertEquals("projects/testDB/databases/(default)",
                capturedRequest.getDatabase());
        
        // Verify we have writes
        Assertions.assertTrue(capturedRequest.getWritesCount() > 0);
        
        // Verify the first write operation is an update
        Write write = capturedRequest.getWrites(0);
        Assertions.assertTrue(write.hasUpdate());
        
        // Verify the document content
        Document firestoreDoc = write.getUpdate();
        Assertions.assertTrue(firestoreDoc.getName().contains("TestDoc"));
        Assertions.assertTrue(firestoreDoc.containsFields("content"));
        Assertions.assertEquals("This is test content", 
                firestoreDoc.getFieldsOrThrow("content").getStringValue());
    }

    @Test
    void testRunGetQuery() {
        // Create a query using the proper constructor 
        Query query = new Query(docStore);
        ServerStream stream = Mockito.mock(ServerStream.class);
        ServerStreamingCallable callable = Mockito.mock(ServerStreamingCallable.class);
        when(mockFirestoreClient.runQueryCallable()).thenReturn(callable);
        when(callable.call(any())).thenReturn(stream);
        when(stream.iterator()).thenReturn(null);
        Assertions.assertInstanceOf(FSDocumentIterator.class, docStore.runGetQuery(query));
    }

    @Test
    void testQueryPlan() {
        // Create a query using the proper constructor
        Query query = new Query(docStore);
        String plan = docStore.queryPlan(query);
        Assertions.assertNotNull(plan);
        Assertions.assertTrue(plan.isEmpty());
    }

    @Test
    void testClose() {
        // First, ensure we can close without exceptions
        docStore.close();
        
        // Verify close was called on the client
        verify(mockFirestoreClient).close();
        
        // Test throwing an exception during close
        FirestoreClient mockClientWithError = mock(FirestoreClient.class);
        Mockito.doThrow(new RuntimeException("Test error")).when(mockClientWithError).close();
        
        FSDocStore docStoreWithErrorOnClose = new FSDocStore.Builder()
                .withFirestoreV1Client(mockClientWithError)
                .withCollectionOptions(collectionOptions)
                .build();
        
        Assertions.assertThrows(RuntimeException.class, docStoreWithErrorOnClose::close);
    }

    @Test
    void testBuilder() throws Exception {
        // Test building with existing client
        FirestoreClient mockClient = mock(FirestoreClient.class);
        FSDocStore store = new FSDocStore.Builder()
                .withFirestoreV1Client(mockClient)
                .withCollectionOptions(collectionOptions)
                .build();
        
        Assertions.assertNotNull(store);
        
        // Test building with credentials
        // Note: This is a limited test since we can't easily mock GoogleCredentials
        FSDocStore.Builder builder = new FSDocStore.Builder()
                .withCollectionOptions(collectionOptions);
        
        Assertions.assertNotNull(builder);
    }
} 