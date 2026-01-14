package com.salesforce.multicloudj.docstore.gcp;

import com.google.api.gax.rpc.FailedPreconditionException;
import com.google.api.gax.rpc.ServerStream;
import com.google.api.gax.rpc.StatusCode;
import com.google.firestore.v1.RunQueryResponse;
import com.salesforce.multicloudj.docstore.client.Query;
import com.salesforce.multicloudj.docstore.driver.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class FSDocumentIteratorTest {

    @Mock
    private FSDocStore mockDocStore;
    
    @Mock
    private QueryRunner mockQueryRunner;
    
    @Mock
    private ServerStream<RunQueryResponse> mockServerStream;
    
    @Mock
    private Iterator<RunQueryResponse> mockIterator;
    
    private Query query;

    @BeforeEach
    void setUp() {
        when(mockQueryRunner.createServerStream()).thenReturn(mockServerStream);
        when(mockServerStream.iterator()).thenReturn(mockIterator);
        query = new Query(mockDocStore);
    }

    private com.google.firestore.v1.Document createFirestoreDocumentWithMultipleFields(String field1, String value1, String field2, String value2) {
        return com.google.firestore.v1.Document.newBuilder()
            .setName("projects/test/databases/(default)/documents/collection/doc1")
            .putFields(field1, com.google.firestore.v1.Value.newBuilder()
                .setStringValue(value1)
                .build())
            .putFields(field2, com.google.firestore.v1.Value.newBuilder()
                .setStringValue(value2)
                .build())
            .build();
    }

    private RunQueryResponse createResponseWithDocument(String field, String value) {
        com.google.firestore.v1.Document firestoreDoc = com.google.firestore.v1.Document.newBuilder()
            .setName("projects/test/databases/(default)/documents/collection/doc1")
            .putFields(field, com.google.firestore.v1.Value.newBuilder()
                .setStringValue(value)
                .build())
            .build();
        return RunQueryResponse.newBuilder()
            .setDocument(firestoreDoc)
            .build();
    }

    private RunQueryResponse createEmptyResponse() {
        return RunQueryResponse.newBuilder().build();
    }

    private Document createDocument() {
        return new Document(new HashMap<>());
    }
    
    @Test
    void testNextWhenDocumentExists() {
        RunQueryResponse response = RunQueryResponse.newBuilder()
            .setDocument(createFirestoreDocumentWithMultipleFields("title", "Test Title", "content", "Test Content"))
            .build();
        
        when(mockIterator.hasNext()).thenReturn(true, false);
        when(mockIterator.next()).thenReturn(response);
        
        FSDocumentIterator iterator = new FSDocumentIterator(mockQueryRunner, query, mockDocStore);
        Document doc = createDocument();
        
        iterator.next(doc);
        
        Assertions.assertEquals("Test Title", doc.getField("title"));
        Assertions.assertEquals("Test Content", doc.getField("content"));
    }

    @Test
    void testNextWhenNoMoreDocuments() {
        when(mockIterator.hasNext()).thenReturn(false);
        
        FSDocumentIterator iterator = new FSDocumentIterator(mockQueryRunner, query, mockDocStore);
        Document doc = createDocument();
        
        NoSuchElementException thrown = Assertions.assertThrows(
            NoSuchElementException.class, 
            () -> iterator.next(doc)
        );
        
        Assertions.assertEquals("No more documents in the iterator", thrown.getMessage());
    }
    
    @Test
    void testHasNextWhenResponsesWithoutDocuments() {
        RunQueryResponse emptyResponse1 = createEmptyResponse();
        RunQueryResponse emptyResponse2 = createEmptyResponse();
        RunQueryResponse responseWithDoc = createResponseWithDocument("field", "value");
        
        when(mockIterator.hasNext()).thenReturn(true, true, true, false);
        when(mockIterator.next()).thenReturn(emptyResponse1, emptyResponse2, responseWithDoc);
        
        FSDocumentIterator iterator = new FSDocumentIterator(mockQueryRunner, query, mockDocStore);
        
        Assertions.assertTrue(iterator.hasNext());
        
        Document doc = createDocument();
        iterator.next(doc);
        
        Assertions.assertEquals("value", doc.getField("field"));
    }

    @Test
    void testHasNextWhenCalledMultipleTimesWithoutConsuming() {
        RunQueryResponse response = createResponseWithDocument("field", "value");
        
        when(mockIterator.hasNext()).thenReturn(true, true, false);
        when(mockIterator.next()).thenReturn(response);
        
        FSDocumentIterator iterator = new FSDocumentIterator(mockQueryRunner, query, mockDocStore);
        
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertTrue(iterator.hasNext());
        
        Document doc = createDocument();
        iterator.next(doc);
        
        Assertions.assertEquals("value", doc.getField("field"));
    }

    @Test
    void testHasNextWhenFailedPreconditionExceptionWithIndexError() {
        StatusCode mockStatusCode = mock(StatusCode.class);
        when(mockStatusCode.getCode()).thenReturn(StatusCode.Code.FAILED_PRECONDITION);
        
        FailedPreconditionException exception = new FailedPreconditionException(
            "FAILED_PRECONDITION: The query requires an index. You can create it here: https://console.firebase.google.com/...",
            null,
            mockStatusCode,
            false
        );
        
        when(mockIterator.hasNext()).thenReturn(true);
        when(mockIterator.next()).thenThrow(exception);
        
        FSDocumentIterator iterator = new FSDocumentIterator(mockQueryRunner, query, mockDocStore);
        
        com.salesforce.multicloudj.common.exceptions.FailedPreconditionException thrown = 
            Assertions.assertThrows(
                com.salesforce.multicloudj.common.exceptions.FailedPreconditionException.class,
                iterator::hasNext
            );
        
        Assertions.assertEquals("The query requires an index.", thrown.getMessage());
        Assertions.assertEquals(exception, thrown.getCause());
    }

    @Test
    void testHasNextWhenFailedPreconditionExceptionWithoutIndexError() {
        StatusCode mockStatusCode = mock(StatusCode.class);
        when(mockStatusCode.getCode()).thenReturn(StatusCode.Code.FAILED_PRECONDITION);
        
        FailedPreconditionException exception = new FailedPreconditionException(
            "FAILED_PRECONDITION: Some other error",
            null,
            mockStatusCode,
            false
        );
        
        when(mockIterator.hasNext()).thenReturn(true);
        when(mockIterator.next()).thenThrow(exception);
        
        FSDocumentIterator iterator = new FSDocumentIterator(mockQueryRunner, query, mockDocStore);
        
        FailedPreconditionException thrown = Assertions.assertThrows(
            FailedPreconditionException.class,
            iterator::hasNext
        );
        
        Assertions.assertEquals(exception, thrown);
    }
    
    @Test
    void testStopWhenIteratorIsActive() {
        RunQueryResponse response = createResponseWithDocument("field", "value");
        
        when(mockIterator.hasNext()).thenReturn(true);
        when(mockIterator.next()).thenReturn(response);
        
        FSDocumentIterator iterator = new FSDocumentIterator(mockQueryRunner, query, mockDocStore);
        
        Assertions.assertTrue(iterator.hasNext());
        
        iterator.stop();
        
        Assertions.assertFalse(iterator.hasNext());
        verify(mockServerStream).cancel();
    }

}
