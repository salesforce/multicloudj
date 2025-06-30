package com.salesforce.multicloudj.docstore.ali;

import com.alicloud.openservices.tablestore.model.sql.SQLQueryRequest;
import com.salesforce.multicloudj.docstore.driver.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AliDocumentIteratorTest {
    QueryRunner runner = mock(QueryRunner.class);
    Document document = mock(Document.class);

    @Test
    void testNext() {
        AliDocumentIterator iter = new AliDocumentIterator(runner, 0, 0);
        iter.run(AliDocumentIterator.INIT_TOKEN);
        when(runner.run(any(), any(), any(), any())).thenReturn(null);
        when(runner.getSqlQueryRequest()).thenReturn(mock(SQLQueryRequest.class));
        Assertions.assertThrows(NoSuchElementException.class, () -> iter.next(document));

        AliDocumentIterator iter2 = new AliDocumentIterator(runner, 2, 5);
        iter2.run(AliDocumentIterator.INIT_TOKEN);
        when(runner.run(any(), any(), any(), any())).thenReturn(null);
        Assertions.assertThrows(NoSuchElementException.class, () -> iter.next(document));
        iter2.stop();

        AliDocumentIterator iter3 = new AliDocumentIterator(runner, 0, 1);
        when(runner.run(any(), any(), any(), any())).thenAnswer(invocation -> {
            // Extract the queryItems parameter (2nd argument in this case).
            List<Map<String, Object>> queryItems = invocation.getArgument(2);

            // Add items to the list.
            Map<String, Object> item1 = new HashMap<>();
            item1.put("key1", "value1");
            item1.put("key2", "value2");
            queryItems.add(item1);

            // Return a token for the first call.
            return "anyToken";
        }).thenReturn(null);
        iter3.run("anyToken");
        // The first call to next should be okay since there is one element
        Assertions.assertDoesNotThrow( () -> iter3.next(document));
        // the second call to next should not get any elements
        Assertions.assertThrows(NoSuchElementException.class, () -> iter3.next(document));
        iter3.stop();
    }

    @Test
    void testNextWithMultiplePages() {
        AliDocumentIterator iter = spy(new AliDocumentIterator(runner, 0, 0));
        when(runner.getSqlQueryRequest()).thenReturn(mock(SQLQueryRequest.class));
        when(runner.run(any(), any(), any(), any())).thenAnswer(invocation -> {
            // Extract the queryItems parameter (2nd argument in this case).
            List<Map<String, Object>> queryItems = invocation.getArgument(2);

            // Add items to the list.
            Map<String, Object> item1 = new HashMap<>();
            item1.put("key12", "value12");
            item1.put("key22", "value22");
            queryItems.add(item1);

            // Return a token for the first call.
            return "anyToken";
        }).thenAnswer(invocation -> {
            // Extract the queryItems parameter (2nd argument in this case).
            List<Map<String, Object>> queryItems = invocation.getArgument(2);

            // Add items to the list.
            Map<String, Object> item1 = new HashMap<>();
            item1.put("key21", "value21");
            item1.put("key23", "value22");
            queryItems.add(item1);

            // Return a token for the second call.
            return "anyToken";
        }).thenReturn(null);
        iter.run("anyToken");
        // The first call to next should be okay since there is one element
        Assertions.assertDoesNotThrow( () -> iter.next(document));
        // There is still one page, verify the hasNext is true even with multiple calls
        Assertions.assertTrue(iter.hasNext());
        Assertions.assertTrue(iter.hasNext());
        Assertions.assertTrue(iter.hasNext());

        // the second call to next should call has next to get the next page
        Assertions.assertDoesNotThrow(() -> iter.next(document));
        verify(iter, times(5)).hasNext();
        verify(runner, times(2)).run(any(), any(), any(), any());

        // now there should be no more pages
        Assertions.assertFalse(iter.hasNext());
        iter.stop();
    }

    @Test
    void testGetPaginationToken() {
        AliDocumentIterator iter = new AliDocumentIterator(runner, 0, 0);
        Assertions.assertNull(iter.getPaginationToken());
    }
}
