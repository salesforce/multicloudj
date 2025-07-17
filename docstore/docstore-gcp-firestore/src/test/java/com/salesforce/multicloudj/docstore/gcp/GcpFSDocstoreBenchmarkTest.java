package com.salesforce.multicloudj.docstore.gcp;

import com.google.cloud.firestore.v1.FirestoreClient;
import com.salesforce.multicloudj.docstore.client.AbstractDocstoreBenchmarkTest;
import com.salesforce.multicloudj.docstore.driver.AbstractDocStore;
import com.salesforce.multicloudj.docstore.driver.CollectionOptions;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.jupiter.api.Disabled;



@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class GcpFSDocstoreBenchmarkTest extends AbstractDocstoreBenchmarkTest {

    private static final Logger logger = LoggerFactory.getLogger(GcpFSDocstoreBenchmarkTest.class);
    private static final String projectId = System.getProperty("gcp.project.id", 
            System.getenv().getOrDefault("GOOGLE_CLOUD_PROJECT", "substrate-sdk-gcp-poc1"));
    private static final String singleKeyCollectionName = "firestore-benchmark-test1";
    private static final String compositeKeyCollectionName = "firestore-benchmark-test2"; 
    private static final String partitionKey = "pName";
    private static final String queryPartitionKey = "Game";
    private static final String querySortKey = "Player";

    @Override
    protected Harness createHarness() {
        return new HarnessImpl();
    }

    public static class HarnessImpl implements Harness {
        FirestoreClient firestoreClient;

        @Override
        public AbstractDocStore createDocStore() throws Exception {
            return createFirestoreDocStore(singleKeyCollectionName, partitionKey, null, "single key");
        }

        @Override
        public AbstractDocStore createQueryDocStore() throws Exception {
            return createFirestoreDocStore(compositeKeyCollectionName, queryPartitionKey, querySortKey, "composite key query");
        }


        private AbstractDocStore createFirestoreDocStore(String collectionName, String partitionKeyName, 
                                                        String sortKeyName, String storeType) throws Exception {
            logger.info("Creating GCP Firestore {} docstore with project: {}, collection: {}", 
                    storeType, projectId, collectionName);
            
            try {
                FirestoreClient client = FirestoreClient.create();
                if (firestoreClient == null) {
                    firestoreClient = client;
                }
                
                logger.info("Successfully created Firestore client for {}", storeType);

                CollectionOptions.CollectionOptionsBuilder optionsBuilder = new CollectionOptions.CollectionOptionsBuilder()
                        .withTableName(getCollectionPath(collectionName))
                        .withPartitionKey(partitionKeyName)
                        .withAllowScans(true);

                if (sortKeyName != null) {
                    optionsBuilder.withSortKey(sortKeyName);
                    logger.debug("Building FSDocStore with collection: {}, partition key: {}, sort key: {}", 
                            collectionName, partitionKeyName, sortKeyName);
                } else {
                    logger.debug("Building FSDocStore with collection: {}, partition key: {}", 
                            collectionName, partitionKeyName);
                }

                CollectionOptions collectionOptions = optionsBuilder.build();

                AbstractDocStore docStore = new FSDocStore().builder()
                        .withFirestoreV1Client(client)
                        .withCollectionOptions(collectionOptions)
                        .build();

                logger.info("Successfully created GCP Firestore {} docstore", storeType);
                return docStore;
                
            } catch (Exception e) {
                logger.error("Failed to create GCP Firestore {} docstore", storeType, e);
                
                // Cleanup on failure
                if (firestoreClient != null) {
                    try {
                        firestoreClient.close();
                        logger.debug("Cleaned up Firestore client after failure");
                    } catch (Exception cleanupException) {
                        logger.warn("Failed to cleanup Firestore client after docstore creation failure", cleanupException);
                    }
                    firestoreClient = null;
                }
                
                if (e instanceof RuntimeException) {
                    throw (RuntimeException) e;
                } else {
                    throw new RuntimeException("Failed to create GCP Firestore " + storeType + " docstore", e);
                }
            }
        }


        private String getCollectionPath(String collectionName) {
            return String.format("projects/%s/databases/(default)/documents/%s", projectId, collectionName);
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