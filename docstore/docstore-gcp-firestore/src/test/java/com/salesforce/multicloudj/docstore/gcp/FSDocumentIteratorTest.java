package com.salesforce.multicloudj.docstore.gcp;

import com.google.api.gax.rpc.ServerStream;
import com.google.firestore.v1.RunQueryResponse;
import com.salesforce.multicloudj.docstore.client.Query;
import com.salesforce.multicloudj.docstore.driver.Document;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

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

    @Test
    void testHasNextWhenExceptionHasNoCause() {
        StatusRuntimeException exception = new StatusRuntimeException(
            Status.UNAVAILABLE.withDescription("Service unavailable")
        );
        
        when(mockIterator.hasNext()).thenReturn(true);
        when(mockIterator.next()).thenThrow(exception);
        
        FSDocumentIterator iterator = new FSDocumentIterator(mockQueryRunner, query, mockDocStore);
        
        RuntimeException thrown = Assertions.assertThrows(RuntimeException.class, iterator::hasNext);
        
        Assertions.assertEquals("UNAVAILABLE: Service unavailable", thrown.getMessage());
        Assertions.assertEquals(exception, thrown.getCause());
    }

    @Test
    void testHasNextWhenExceptionCauseHasMessage() {
        Exception rootCause = new Exception("Connection refused");
        StatusRuntimeException exception = new StatusRuntimeException(
            Status.UNAVAILABLE.withDescription("Service unavailable").withCause(rootCause)
        );
        
        when(mockIterator.hasNext()).thenReturn(true);
        when(mockIterator.next()).thenThrow(exception);
        
        FSDocumentIterator iterator = new FSDocumentIterator(mockQueryRunner, query, mockDocStore);
        
        RuntimeException thrown = Assertions.assertThrows(RuntimeException.class, iterator::hasNext);
        
        Assertions.assertEquals("Connection refused", thrown.getMessage());
        Assertions.assertEquals(exception, thrown.getCause());
    }

    @Test
    void testHasNextWhenExceptionCauseMessageIsNull() {
        Exception rootCause = new Exception((String) null);
        StatusRuntimeException exception = new StatusRuntimeException(
            Status.UNAVAILABLE.withDescription("Service unavailable").withCause(rootCause)
        );
        
        when(mockIterator.hasNext()).thenReturn(true);
        when(mockIterator.next()).thenThrow(exception);
        
        FSDocumentIterator iterator = new FSDocumentIterator(mockQueryRunner, query, mockDocStore);
        
        RuntimeException thrown = Assertions.assertThrows(RuntimeException.class, iterator::hasNext);
        
        Assertions.assertEquals("UNAVAILABLE: Service unavailable", thrown.getMessage());
        Assertions.assertEquals(exception, thrown.getCause());
    }

    @Test
    void testNextWhenDocumentExists() {
        com.google.firestore.v1.Document firestoreDoc = com.google.firestore.v1.Document.newBuilder()
            .setName("projects/test/databases/(default)/documents/collection/doc1")
            .putFields("title", com.google.firestore.v1.Value.newBuilder()
                .setStringValue("Test Title")
                .build())
            .putFields("content", com.google.firestore.v1.Value.newBuilder()
                .setStringValue("Test Content")
                .build())
            .build();
        
        RunQueryResponse response = RunQueryResponse.newBuilder()
            .setDocument(firestoreDoc)
            .build();
        
        when(mockIterator.hasNext()).thenReturn(true, false);
        when(mockIterator.next()).thenReturn(response);
        
        FSDocumentIterator iterator = new FSDocumentIterator(mockQueryRunner, query, mockDocStore);
        Document doc = new Document(new HashMap<>());
        
        iterator.next(doc);
        
        Assertions.assertEquals("Test Title", doc.getField("title"));
        Assertions.assertEquals("Test Content", doc.getField("content"));
    }

    @Test
    void testNextWhenNoMoreDocuments() {
        when(mockIterator.hasNext()).thenReturn(false);
        
        FSDocumentIterator iterator = new FSDocumentIterator(mockQueryRunner, query, mockDocStore);
        Document doc = new Document(new HashMap<>());
        
        NoSuchElementException thrown = Assertions.assertThrows(
            NoSuchElementException.class, 
            () -> iterator.next(doc)
        );
        
        Assertions.assertEquals("No more documents in the iterator", thrown.getMessage());
    }

}
