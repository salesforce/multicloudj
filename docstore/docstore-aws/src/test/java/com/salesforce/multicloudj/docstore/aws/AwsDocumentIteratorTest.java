package com.salesforce.multicloudj.docstore.aws;

import com.salesforce.multicloudj.docstore.driver.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.NoSuchElementException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AwsDocumentIteratorTest {
    QueryRunner runner = mock(QueryRunner.class);
    Document document = mock(Document.class);

    @Test
    void testNext() {
        AwsDocumentIterator iter = new AwsDocumentIterator(runner, 0, 0, 0);
        iter.run(null);
        when(runner.run(any(), any(), any())).thenReturn(null);
        Assertions.assertThrows(NoSuchElementException.class, () -> iter.next(document));

        AwsDocumentIterator iter2 = new AwsDocumentIterator(runner, 2, 5, 0);
        iter.run(null);
        when(runner.run(any(), any(), any())).thenReturn(null);
        Assertions.assertThrows(NoSuchElementException.class, () -> iter.next(document));
        iter.stop();
    }
}
