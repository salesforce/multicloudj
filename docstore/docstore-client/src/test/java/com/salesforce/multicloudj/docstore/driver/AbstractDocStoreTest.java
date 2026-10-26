package com.salesforce.multicloudj.docstore.driver;

import com.google.protobuf.Timestamp;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.retries.RetryConfig;
import com.salesforce.multicloudj.docstore.client.Query;
import com.salesforce.multicloudj.docstore.driver.testtypes.Book;
import com.salesforce.multicloudj.docstore.driver.testtypes.Person;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AbstractDocStoreTest {
  static class TestDocStore extends AbstractDocStore {

    public TestDocStore() {
      super(new Builder());
    }

    public TestDocStore(Builder builder) {
      super(builder);
    }

    @Override
    public Builder builder() {
      return new Builder();
    }

    @Override
    public SubstrateSdkException mapException(Throwable t) {
      return null;
    }

    @Override
    public Object getKey(Document document) {
      return document.getField("title");
    }

    @Override
    public void runActions(List<Action> actions, Consumer<Predicate<Object>> beforeDo) {}

    @Override
    public DocumentIterator runGetQuery(Query query) {
      return null;
    }

    @Override
    public String queryPlan(Query query) {
      return "";
    }

    @Override
    public void close() {
      this.closed = true;
    }

    @Override
    protected void batchGet(
        List<Action> actions, Consumer<Predicate<Object>> beforeDo, int start, int end) {}

    public static class Builder extends AbstractDocStore.Builder<TestDocStore, Builder> {

      public Builder() {
        providerId("aws");
      }

      @Override
      public Builder self() {
        return this;
      }

      @Override
      public TestDocStore build() {
        return new TestDocStore(this);
      }
    }
  }

  class TestAction extends Action {
    public TestAction(
        ActionKind kind, Document document, List<String> fieldPaths, Map<String, Object> mods) {
      super(kind, document, fieldPaths, mods, false);
    }

    public void setKey(Object key) {
      this.key = key;
    }

    public void setIndex(int index) {
      this.index = index;
    }
  }

  TestDocStore docStore;

  @BeforeEach
  void setUp() {
    docStore = new TestDocStore();
    CollectionOptions collectionOptions =
        new CollectionOptions.CollectionOptionsBuilder()
            .withTableName("table")
            .withPartitionKey("title")
            .withSortKey("publisher")
            .withAllowScans(false)
            .withRevisionField("docRevision")
            .withMaxOutstandingActionRPCs(10)
            .build();
    try {
      Field field = docStore.getClass().getSuperclass().getDeclaredField("collectionOptions");
      field.setAccessible(true);
      field.set(docStore, collectionOptions);
    } catch (Exception e) {
      Assertions.fail("Failed to get field.");
    }
  }

  @Test
  void testGetRevisionField() {
    Assertions.assertEquals("docRevision", docStore.getRevisionField());
    TestDocStore docStoreDefaultRevision = new TestDocStore();
    CollectionOptions collectionOptions =
        new CollectionOptions.CollectionOptionsBuilder()
            .withTableName("table")
            .withPartitionKey("title")
            .withSortKey("publisher")
            .withAllowScans(false)
            .withMaxOutstandingActionRPCs(10)
            .build();
    try {
      Field field =
          docStoreDefaultRevision.getClass().getSuperclass().getDeclaredField("collectionOptions");
      field.setAccessible(true);
      field.set(docStoreDefaultRevision, collectionOptions);
    } catch (Exception e) {
      Assertions.fail("Failed to get field.");
    }
    Assertions.assertEquals("DocstoreRevision", docStoreDefaultRevision.getRevisionField());
  }

  @Test
  void testCheckClosed() {
    docStore.close();
    Assertions.assertThrows(IllegalStateException.class, () -> docStore.checkClosed());
  }

  @Test
  void testToDriverAction() {
    Person person =
        new Person(
            Collections.singleton("Jamie"),
            "Zoe",
            "Ford",
            Timestamp.newBuilder().setNanos(100).build());
    Book book =
        new Book(
            "YellowBook",
            person,
            "WA",
            Timestamp.newBuilder().setNanos(1000).build(),
            3.99f,
            new HashMap<>(Map.of("Chapter 1", 5, "Chapter 2", 10)),
            null);

    // Test no key is found.
    TestAction get = new TestAction(ActionKind.ACTION_KIND_GET, new Document(person), null, null);
    Assertions.assertThrows(IllegalArgumentException.class, () -> docStore.toDriverAction(get));

    // Test toDriverMods with null value
    TestAction update =
        new TestAction(ActionKind.ACTION_KIND_UPDATE, new Document(book), null, null);
    Assertions.assertThrows(IllegalArgumentException.class, () -> docStore.toDriverAction(update));

    // Test toDriverMods
    TestAction update2 =
        new TestAction(
            ActionKind.ACTION_KIND_UPDATE,
            new Document(book),
            null,
            Map.of("price", 4, "author.lastName", "Frank"));
    Assertions.assertDoesNotThrow(() -> docStore.toDriverAction(update2));
  }

  @Test
  void testWithRetryConfigStoresValidConfig() {
    RetryConfig retryConfig =
        RetryConfig.builder()
            .mode(RetryConfig.Mode.FIXED)
            .maxAttempts(3)
            .fixedDelayMillis(100)
            .build();
    TestDocStore.Builder builder = new TestDocStore.Builder().withRetryConfig(retryConfig);
    Assertions.assertSame(retryConfig, builder.getRetryConfig());
  }

  @Test
  void testWithRetryConfigAcceptsNull() {
    TestDocStore.Builder builder = new TestDocStore.Builder().withRetryConfig(null);
    Assertions.assertNull(builder.getRetryConfig());
  }

  @Test
  void testWithRetryConfigRejectsInvalidConfig() {
    RetryConfig retryConfig = RetryConfig.builder().maxAttempts(0).build();
    Assertions.assertThrows(
        InvalidArgumentException.class,
        () -> new TestDocStore.Builder().withRetryConfig(retryConfig));
  }
}
