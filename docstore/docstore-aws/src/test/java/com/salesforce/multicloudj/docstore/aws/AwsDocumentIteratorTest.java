package com.salesforce.multicloudj.docstore.aws;

import com.salesforce.multicloudj.docstore.client.Query;
import com.salesforce.multicloudj.docstore.driver.CollectionOptions;
import com.salesforce.multicloudj.docstore.driver.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.ArrayList;
import java.util.NoSuchElementException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AwsDocumentIteratorTest {
    QueryRunner runner = mock(QueryRunner.class);
    Document document = mock(Document.class);

    @Test
    void testNext() {
        Query query = mock(Query.class);
        when(query.getOffset()).thenReturn(0);
        when(query.getLimit()).thenReturn(0);
        AwsDocumentIterator iter = new AwsDocumentIterator(runner, query, 0);
        iter.run(null);
        when(runner.run(any(), any(), any())).thenReturn(null);
        Assertions.assertThrows(NoSuchElementException.class, () -> iter.next(document));

        Query query2 = mock(Query.class);
        when(query2.getOffset()).thenReturn(2);
        when(query2.getLimit()).thenReturn(5);
        AwsDocumentIterator iter2 = new AwsDocumentIterator(runner, query2, 0);
        iter2.run(null);
        when(runner.run(any(), any(), any())).thenReturn(null);
        Assertions.assertThrows(NoSuchElementException.class, () -> iter2.next(document));
        iter2.stop();

        Query query3 = mock(Query.class);
        AwsPaginationToken paginationToken = mock(AwsPaginationToken.class);
        when(query3.getPaginationToken()).thenReturn(paginationToken);
        when(query3.getLimit()).thenReturn(5);
        AwsDocumentIterator iter3 = new AwsDocumentIterator(runner, query3, 0);
        iter3.run(null);
        when(runner.run(any(), any(), any())).thenReturn(null);
        Assertions.assertThrows(NoSuchElementException.class, () -> iter3.next(document));
        iter.stop();
    }
}
