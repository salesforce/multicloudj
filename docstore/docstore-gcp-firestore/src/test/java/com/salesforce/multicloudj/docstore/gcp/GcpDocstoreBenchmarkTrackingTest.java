package com.salesforce.multicloudj.docstore.gcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.salesforce.multicloudj.docstore.client.AbstractDocstoreBenchmarkTest;
import com.salesforce.multicloudj.docstore.client.DocStoreClient;
import java.lang.reflect.Field;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Verifies the teardown-tracking contract of {@code benchmarkWriteReadDelete} through the concrete
 * GCP benchmark: a benchmark that writes under a fresh key must register that key in {@code
 * benchmarkCreatedKeys} so teardown can delete it even if the invocation fails partway. The
 * docstore client is mocked (no network) and setup is skipped; the private/protected benchmark
 * internals are reached via reflection.
 */
class GcpDocstoreBenchmarkTrackingTest {

  @Test
  @SuppressWarnings("unchecked")
  void writeReadDeleteTracksItsKeyForTeardown() throws Exception {
    GcpFirestoreDocstoreBenchmarkTest bench = new GcpFirestoreDocstoreBenchmarkTest();

    Field clientField = AbstractDocstoreBenchmarkTest.class.getDeclaredField("docStoreClient");
    clientField.setAccessible(true);
    clientField.set(bench, mock(DocStoreClient.class)); // put/get/delete become no-ops

    bench.benchmarkWriteReadDelete(mock(Blackhole.class));

    Field keysField = AbstractDocstoreBenchmarkTest.class.getDeclaredField("benchmarkCreatedKeys");
    keysField.setAccessible(true);
    Set<String> keys = (Set<String>) keysField.get(bench);

    assertEquals(1, keys.size(), "write-read-delete should track exactly one key");
    assertTrue(
        keys.iterator().next().startsWith("writereaddeletebenchmark-player-"),
        "the tracked key should be the write-read-delete key so teardown can clean it up");
  }
}
