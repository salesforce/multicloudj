package com.salesforce.multicloudj.blob.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.salesforce.multicloudj.blob.driver.AbstractBlobStore;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;

/**
 * Verifies {@link AbstractBlobBenchmarkTest#stageCorpusFor(String)} against a recording store,
 * with no cloud credentials. This is where the staging guarantee has teeth: it catches a read-path
 * benchmark that is classified but not wired into the switch (so it would silently measure against
 * an empty corpus), which the name classification alone cannot.
 */
public class BlobBenchmarkStagingTest {

  /** Concrete subclass so the test can drive the staging seam; the harness is never created. */
  public static class Benchmarks extends AbstractBlobBenchmarkTest {
    @Override
    protected Harness createHarness() {
      throw new UnsupportedOperationException("staging tests do not create a real harness");
    }

    @Override
    protected String getProviderId() {
      return "test";
    }
  }

  /** Stages the corpus for one benchmark against a mock store and returns the uploaded keys. */
  private static List<String> stage(String benchmarkMethod) {
    List<String> uploadedKeys = Collections.synchronizedList(new ArrayList<>());
    AbstractBlobStore store = mock(AbstractBlobStore.class);
    when(store.upload(any(UploadRequest.class), any(InputStream.class)))
        .thenAnswer(inv -> {
          uploadedKeys.add(inv.<UploadRequest>getArgument(0).getKey());
          return null;
        });
    Benchmarks benchmarks = new Benchmarks();
    benchmarks.bucketClient = new BucketClient(store);
    benchmarks.initBlobPayloads();
    benchmarks.stageCorpusFor(benchmarkMethod);
    return uploadedKeys;
  }

  @Test
  void everyBenchmarkIsClassifiedAndStagesAccordingly() {
    Set<String> declared = new HashSet<>();
    for (Method method : AbstractBlobBenchmarkTest.class.getMethods()) {
      if (method.isAnnotationPresent(Benchmark.class)) {
        declared.add(method.getName());
      }
    }
    Set<String> classified = new HashSet<>();
    classified.addAll(AbstractBlobBenchmarkTest.READ_PATH_BENCHMARKS);
    classified.addAll(AbstractBlobBenchmarkTest.WRITE_PATH_BENCHMARKS);
    assertEquals(classified, declared,
        "every @Benchmark must be in READ_PATH_BENCHMARKS or WRITE_PATH_BENCHMARKS");

    for (String method : AbstractBlobBenchmarkTest.READ_PATH_BENCHMARKS) {
      assertFalse(stage(method).isEmpty(),
          method + " is read-path but staged nothing — missing stageCorpusFor() case?");
    }
    for (String method : AbstractBlobBenchmarkTest.WRITE_PATH_BENCHMARKS) {
      assertTrue(stage(method).isEmpty(),
          method + " is write-path but stageCorpusFor() staged objects");
    }
  }

  @Test
  void readPathBenchmarksStageExpectedSizeAndPrefix() {
    assertStages("benchmarkDownloadSmall", 100, AbstractBlobBenchmarkTest.SMALL_BLOBS_PREFIX);
    assertStages("benchmarkGetMetadata", 100, AbstractBlobBenchmarkTest.SMALL_BLOBS_PREFIX);
    assertStages("benchmarkList", 100, AbstractBlobBenchmarkTest.SMALL_BLOBS_PREFIX);
    assertStages("benchmarkListPage", 100, AbstractBlobBenchmarkTest.SMALL_BLOBS_PREFIX);
    assertStages("benchmarkDownloadMedium", 20, AbstractBlobBenchmarkTest.MEDIUM_BLOBS_PREFIX);
    assertStages("benchmarkDownloadLarge", 5, AbstractBlobBenchmarkTest.LARGE_BLOBS_PREFIX);
    assertStages("benchmarkCopy", 1, AbstractBlobBenchmarkTest.SMALL_BLOBS_PREFIX);
  }

  private static void assertStages(String method, int expectedCount, String expectedPrefix) {
    List<String> keys = stage(method);
    assertEquals(expectedCount, keys.size(), method + " staged an unexpected object count");
    assertTrue(keys.stream().allMatch(key -> key.startsWith(expectedPrefix)),
        method + " staged keys outside prefix " + expectedPrefix);
  }
}
