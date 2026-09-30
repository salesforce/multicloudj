package com.salesforce.multicloudj.docstore.gcp;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;

import com.google.cloud.firestore.v1.FirestoreClient;
import com.salesforce.multicloudj.docstore.client.AbstractDocstoreBenchmarkTest;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/**
 * Lifecycle verification for {@link GcpFirestoreDocstoreBenchmarkTest.HarnessImpl}. Mocks the
 * static {@code FirestoreClient.create()} factory so no network/credentials are required.
 */
class GcpFirestoreHarnessTest {

  private static final String[] PROPS = {
    "DOCSTORE_BENCHMARK_GCP_PROJECT_ID",
    "DOCSTORE_BENCHMARK_GCP_SINGLE_KEY_COLLECTION",
    "DOCSTORE_BENCHMARK_GCP_COMPOSITE_KEY_COLLECTION"
  };

  @BeforeEach
  void setProps() {
    System.setProperty("DOCSTORE_BENCHMARK_GCP_PROJECT_ID", "test-project");
    System.setProperty("DOCSTORE_BENCHMARK_GCP_SINGLE_KEY_COLLECTION", "players");
    System.setProperty("DOCSTORE_BENCHMARK_GCP_COMPOSITE_KEY_COLLECTION", "games");
  }

  @AfterEach
  void clearProps() {
    for (String p : PROPS) {
      System.clearProperty(p);
    }
  }

  /**
   * Every FirestoreClient the harness opens must be closed. The harness builds a single-key store
   * and a composite-key store; {@code close()} must release every client it created. Fails if any
   * opened client is never closed (the leak).
   */
  @Test
  void closesEveryFirestoreClientItOpens() throws Exception {
    List<FirestoreClient> created = new ArrayList<>();
    try (MockedStatic<FirestoreClient> statics = mockStatic(FirestoreClient.class)) {
      statics
          .when(FirestoreClient::create)
          .thenAnswer(
              inv -> {
                FirestoreClient c = mock(FirestoreClient.class);
                created.add(c);
                return c;
              });

      AbstractDocstoreBenchmarkTest.Harness harness =
          new GcpFirestoreDocstoreBenchmarkTest.HarnessImpl();
      harness.createDocStore();
      harness.createQueryDocStore();
      harness.close();
    }

    assertFalse(created.isEmpty(), "harness should have created at least one FirestoreClient");
    for (FirestoreClient c : created) {
      verify(c).close();
    }
  }
}
