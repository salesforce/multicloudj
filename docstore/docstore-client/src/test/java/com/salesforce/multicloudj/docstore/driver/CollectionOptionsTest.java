package com.salesforce.multicloudj.docstore.driver;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CollectionOptionsTest {
    @Test
    void testCollectionOptions() {
        CollectionOptions collectionOptions = new CollectionOptions.CollectionOptionsBuilder()
                .withTableName("table")
                .withPartitionKey("title")
                .withSortKey("publisher")
                .withAllowScans(false)
                .withRevisionField("docRevision")
                .withMaxOutstandingActionRPCs(10)
                .build();

        Assertions.assertEquals("table", collectionOptions.getTableName());
    }
}
