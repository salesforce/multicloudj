package com.salesforce.multicloudj.docstore.gcp;

import com.google.cloud.firestore.v1.FirestoreClient;
import com.salesforce.multicloudj.docstore.client.AbstractDocstoreBenchmarkTest;
import com.salesforce.multicloudj.docstore.driver.AbstractDocStore;
import com.salesforce.multicloudj.docstore.driver.CollectionOptions;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class GcpFirestoreDocstoreBenchmarkTest extends AbstractDocstoreBenchmarkTest {

  private static final Logger logger =
      LoggerFactory.getLogger(GcpFirestoreDocstoreBenchmarkTest.class);

  @Override
  protected String getProviderId() {
    return "gcp-firestore";
  }

  @Override
  protected Harness createHarness() {
    return new HarnessImpl();
  }

  public static class HarnessImpl implements Harness {
    FirestoreClient firestoreClient;

    @Override
    public AbstractDocStore createDocStore() throws Exception {
      return createFirestoreDocStore(
          requireEnv("DOCSTORE_BENCHMARK_GCP_SINGLE_KEY_COLLECTION"),
          "pName",
          null,
          "single key");
    }

    @Override
    public AbstractDocStore createQueryDocStore() throws Exception {
      return createFirestoreDocStore(
          requireEnv("DOCSTORE_BENCHMARK_GCP_COMPOSITE_KEY_COLLECTION"),
          "Game",
          "Player",
          "composite key query");
    }

    private AbstractDocStore createFirestoreDocStore(
        String collectionName, String partitionKeyName, String sortKeyName, String storeType)
        throws Exception {
      String projectId = requireEnv("DOCSTORE_BENCHMARK_GCP_PROJECT_ID");
      logger.info(
          "Creating GCP Firestore {} docstore with project: {}, collection: {}",
          storeType,
          projectId,
          collectionName);

      try {
        FirestoreClient client = FirestoreClient.create();
        if (firestoreClient == null) {
          firestoreClient = client;
        }

        CollectionOptions.CollectionOptionsBuilder optionsBuilder =
            new CollectionOptions.CollectionOptionsBuilder()
                .withTableName(getCollectionPath(projectId, collectionName))
                .withPartitionKey(partitionKeyName)
                .withAllowScans(true);

        if (sortKeyName != null) {
          optionsBuilder.withSortKey(sortKeyName);
        }

        CollectionOptions collectionOptions = optionsBuilder.build();

        AbstractDocStore docStore =
            new FSDocStore()
                .builder()
                .withFirestoreV1Client(client)
                .withCollectionOptions(collectionOptions)
                .build();

        logger.info("Successfully created GCP Firestore {} docstore", storeType);
        return docStore;

      } catch (Exception e) {
        logger.error("Failed to create GCP Firestore {} docstore", storeType, e);

        if (firestoreClient != null) {
          try {
            firestoreClient.close();
          } catch (Exception cleanupException) {
            logger.warn("Failed to cleanup Firestore client after failure", cleanupException);
          }
          firestoreClient = null;
        }

        if (e instanceof RuntimeException) {
          throw (RuntimeException) e;
        } else {
          throw new RuntimeException(
              "Failed to create GCP Firestore " + storeType + " docstore", e);
        }
      }
    }

    private String getCollectionPath(String projectId, String collectionName) {
      return String.format(
          "projects/%s/databases/(default)/documents/%s", projectId, collectionName);
    }

    @Override
    public void close() throws Exception {
      if (firestoreClient != null) {
        firestoreClient.close();
        logger.debug("Closed Firestore client");
      }
    }
  }
}
