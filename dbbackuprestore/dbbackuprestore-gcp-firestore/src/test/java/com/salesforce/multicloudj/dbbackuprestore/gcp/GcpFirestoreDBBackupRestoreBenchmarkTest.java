package com.salesforce.multicloudj.dbbackuprestore.gcp;

import com.google.cloud.firestore.v1.FirestoreAdminClient;
import com.salesforce.multicloudj.common.gcp.GcpConstants;
import com.salesforce.multicloudj.common.util.UUID;
import com.salesforce.multicloudj.dbbackuprestore.client.AbstractDBBackupRestoreBenchmarkTest;
import com.salesforce.multicloudj.dbbackuprestore.client.DBBackupRestoreClient;
import java.io.IOException;
import org.junit.jupiter.api.TestInstance;

/**
 * Hits the real Firestore Admin endpoint directly, so it requires live Application Default
 * Credentials (GOOGLE_APPLICATION_CREDENTIALS) and network access.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class GcpFirestoreDBBackupRestoreBenchmarkTest extends AbstractDBBackupRestoreBenchmarkTest {

  @Override
  protected Harness createHarness() {
    return new HarnessImpl();
  }

  @Override
  protected String getProviderId() {
    return GcpConstants.PROVIDER_ID;
  }

  public static class HarnessImpl implements Harness {

    private final String location = requireEnv("DBBACKUPRESTORE_BENCHMARK_GCP_LOCATION");
    private final String databaseName = requireEnv("DBBACKUPRESTORE_BENCHMARK_GCP_DATABASE_NAME");
    private FirestoreAdminClient firestoreAdminClient;

    @Override
    public DBBackupRestoreClient createClient() {
      try {
        firestoreAdminClient = FirestoreAdminClient.create();
      } catch (IOException e) {
        throw new RuntimeException("Failed to create the firestore admin client", e);
      }

      FSDBBackupRestore dbBackupRestore =
          new FSDBBackupRestore.Builder()
              .withFirestoreAdminClient(firestoreAdminClient)
              .withRegion(location)
              .withResourceName(databaseName)
              .build();

      return new DBBackupRestoreClient(dbBackupRestore);
    }

    @Override
    public String getTargetResource() {
      return "restored-db-benchmark-" + UUID.uniqueString();
    }

    @Override
    public void close() {
      if (firestoreAdminClient != null) {
        firestoreAdminClient.close();
      }
    }
  }
}
