package com.salesforce.multicloudj.docstore.driver;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CollectionOptionsTest {
  @Test
  void testCollectionOptions() {
    CollectionOptions collectionOptions =
        new CollectionOptions.CollectionOptionsBuilder()
            .withTableName("table")
            .withPartitionKey("title")
            .withSortKey("publisher")
            .withAllowScans(false)
            .withRevisionField("docRevision")
            .withMaxOutstandingActionRPCs(10)
            .build();

    Assertions.assertEquals("table", collectionOptions.getTableName());
    Assertions.assertFalse(collectionOptions.isConsistentRead());
  }

  @Test
  void testConsistentReadOption() {
    Assertions.assertTrue(
        new CollectionOptions.CollectionOptionsBuilder().withConsistentRead(true).build()
            .isConsistentRead());
    Assertions.assertFalse(
        new CollectionOptions.CollectionOptionsBuilder().withConsistentRead(false).build()
            .isConsistentRead());
    Assertions.assertFalse(
        new CollectionOptions.CollectionOptionsBuilder().withConsistentRead(null).build()
            .isConsistentRead());
  }
}
