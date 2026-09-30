package com.salesforce.multicloudj.docstore.gcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.salesforce.multicloudj.docstore.client.AbstractDocstoreBenchmarkTest;
import com.salesforce.multicloudj.docstore.client.DocStoreClient;
import com.salesforce.multicloudj.docstore.driver.ActionList;
import com.salesforce.multicloudj.docstore.driver.Document;
import java.lang.reflect.Field;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Regression coverage for the teardown-tracking contract of {@code benchmarkWriteReadDelete}: when
 * the write succeeds but a later op fails, the key must already be registered in {@code
 * benchmarkCreatedKeys} so teardown deletes the orphaned document. This test fails if the
 * registration is moved after the failing call. The datastore is mocked (no network); benchmark
 * internals are reached via reflection.
 */
class GcpDocstoreBenchmarkTrackingTest {

  @Test
  void teardownDeletesKeyWhenWriteReadDeleteFailsAfterWrite() throws Exception {
    GcpFirestoreDocstoreBenchmarkTest bench = new GcpFirestoreDocstoreBenchmarkTest();

    // put + get succeed; delete throws — the doc is written but the invocation aborts before its
    // own delete, so teardown must clean it up. Regresses if the key is tracked only after a call
    // that can fail.
    DocStoreClient client = mock(DocStoreClient.class);
    doThrow(new RuntimeException("simulated delete failure"))
        .when(client)
        .delete(any(Document.class));
    ActionList teardownActions = mock(ActionList.class);
    when(client.getActions()).thenReturn(teardownActions);

    setField(bench, "docStoreClient", client);

    assertThrows(
        RuntimeException.class, () -> bench.benchmarkWriteReadDelete(mock(Blackhole.class)));

    Set<String> tracked = trackedKeys(bench);
    assertEquals(1, tracked.size(), "the written key must be tracked despite the failed delete");
    assertTrue(
        tracked.iterator().next().startsWith("writereaddeletebenchmark-player-"),
        "the tracked key should be the write-read-delete key");

    // teardown must issue a cleanup delete for the one orphaned key
    bench.teardownBenchmark();
    verify(teardownActions, times(1)).delete(any(Document.class));
    verify(teardownActions).run();
  }

  private static void setField(Object target, String name, Object value) throws Exception {
    Field f = AbstractDocstoreBenchmarkTest.class.getDeclaredField(name);
    f.setAccessible(true);
    f.set(target, value);
  }

  @SuppressWarnings("unchecked")
  private static Set<String> trackedKeys(Object bench) throws Exception {
    Field f = AbstractDocstoreBenchmarkTest.class.getDeclaredField("benchmarkCreatedKeys");
    f.setAccessible(true);
    return (Set<String>) f.get(bench);
  }
}
