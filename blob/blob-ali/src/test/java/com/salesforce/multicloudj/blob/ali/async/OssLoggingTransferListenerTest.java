package com.salesforce.multicloudj.blob.ali.async;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.salesforce.multicloudj.blob.ali.async.OssLoggingTransferListener.Direction;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;

class OssLoggingTransferListenerTest {

  @Test
  void onProgressAccumulatesIncrementIntoSharedCounter() {
    AtomicLong cumulative = new AtomicLong(0L);
    OssLoggingTransferListener listener =
        OssLoggingTransferListener.create(Direction.UPLOAD, "key1", cumulative, true);

    // onProgress(increment, cumulativeWritten, total) — the listener accumulates arg1.
    listener.onProgress(100L, 100L, 500L);
    listener.onProgress(150L, 250L, 500L);

    assertEquals(250L, cumulative.get());
  }

  @Test
  void onProgressAccumulatesEvenWhenLoggingDisabled() {
    // Byte accounting must always run so totalBytesTransferred is populated
    // regardless of the transferStatusLoggingEnabled flag.
    AtomicLong cumulative = new AtomicLong(0L);
    OssLoggingTransferListener listener =
        OssLoggingTransferListener.create(Direction.DOWNLOAD, "key2", cumulative, false);

    listener.onProgress(1024L, 1024L, 1024L);
    listener.onFinish();

    assertEquals(1024L, cumulative.get());
  }

  @Test
  void transferFailedDoesNotAlterCounter() {
    AtomicLong cumulative = new AtomicLong(42L);
    OssLoggingTransferListener listener =
        OssLoggingTransferListener.create(Direction.UPLOAD, "key3", cumulative, true);

    listener.transferFailed(new RuntimeException("boom"));

    assertEquals(42L, cumulative.get());
  }

  @Test
  void multipleListenersShareTheSameDirectoryScopedCounter() {
    // Mirrors AWS's shared totalBytesTransferred across all files in a directory op.
    AtomicLong cumulative = new AtomicLong(0L);
    OssLoggingTransferListener fileA =
        OssLoggingTransferListener.create(Direction.UPLOAD, "a.txt", cumulative, true);
    OssLoggingTransferListener fileB =
        OssLoggingTransferListener.create(Direction.UPLOAD, "b.txt", cumulative, true);

    fileA.onProgress(300L, 300L, 300L);
    fileB.onProgress(700L, 700L, 700L);

    assertEquals(1000L, cumulative.get());
  }

  @Test
  void concurrentOnProgressAccumulatesAtomically() throws Exception {
    AtomicLong cumulative = new AtomicLong(0L);
    int threads = 16;
    int incrementsPerThread = 1000;
    long incrementSize = 8L;

    List<CompletableFuture<Void>> futures = IntStream.range(0, threads)
        .mapToObj(t -> CompletableFuture.runAsync(() -> {
          OssLoggingTransferListener listener = OssLoggingTransferListener.create(
              Direction.DOWNLOAD, "key-" + t, cumulative, false);
          for (int i = 0; i < incrementsPerThread; i++) {
            listener.onProgress(incrementSize, incrementSize * (i + 1), 0L);
          }
        }))
        .collect(Collectors.toList());
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();

    assertEquals((long) threads * incrementsPerThread * incrementSize, cumulative.get());
  }

  @Test
  void onFinishIsSafeToCallWithoutPriorProgress() {
    AtomicLong cumulative = new AtomicLong(0L);
    OssLoggingTransferListener listener =
        OssLoggingTransferListener.create(Direction.DOWNLOAD, "empty", cumulative, true);

    // Should not throw and should leave the counter untouched.
    listener.onFinish();

    assertEquals(0L, cumulative.get());
  }

  // ---------------------------------------------------------------------------
  // Logging emission / gating (logger injected via the package-private constructor)
  // ---------------------------------------------------------------------------

  @Test
  void loggingEnabledEmitsExpectedLevelsPerLifecycleEvent() {
    Logger logger = mock(Logger.class);
    AtomicLong cumulative = new AtomicLong(0L);

    // Construction with logging enabled emits the "transfer initiated" DEBUG line.
    OssLoggingTransferListener listener = new OssLoggingTransferListener(
        logger, Direction.UPLOAD, "key", cumulative, true);
    verify(logger).debug(anyString(), any(), any());

    // onProgress -> TRACE (direction, key, increment, transferred, total)
    listener.onProgress(100L, 100L, 200L);
    verify(logger).trace(anyString(), any(), any(), any(), any(), any());

    // onFinish -> DEBUG (direction, key, fileBytes, directoryCumulativeBytes, elapsed).
    // Per-file completion is high-volume on large directories, so it is emitted at DEBUG
    // to avoid swamping production log appender queues.
    listener.onFinish();
    verify(logger).debug(anyString(), any(), any(), any(), any(), any());

    // transferFailed -> ERROR (direction, key, fileBytes, directoryCumulativeBytes, elapsed, ex)
    RuntimeException ex = new RuntimeException("boom");
    listener.transferFailed(ex);
    verify(logger).error(anyString(), any(), any(), any(), any(), any(), any());
  }

  @Test
  void loggingDisabledNeverTouchesLogger() {
    Logger logger = mock(Logger.class);
    AtomicLong cumulative = new AtomicLong(0L);

    OssLoggingTransferListener listener = new OssLoggingTransferListener(
        logger, Direction.DOWNLOAD, "key", cumulative, false);
    listener.onProgress(100L, 100L, 200L);
    listener.onFinish();
    listener.transferFailed(new RuntimeException("boom"));

    // No log line at any level when the flag is off — but byte accounting still ran.
    verifyNoInteractions(logger);
    assertEquals(100L, cumulative.get());
  }

  @Test
  void loggingDisabledStillAccumulatesButDebugInitiatedSuppressed() {
    Logger logger = mock(Logger.class);
    AtomicLong cumulative = new AtomicLong(0L);

    // Construction with logging disabled must NOT emit the initiated DEBUG line.
    new OssLoggingTransferListener(
        logger, Direction.UPLOAD, "key", cumulative, false);

    verify(logger, never()).debug(anyString(), any(), any());
  }

  @Test
  void onFinishLogsPerFileBytesDistinctFromDirectoryCumulative() {
    // Shared directory counter already holds another file's bytes; this listener should
    // log its OWN per-file total separately from the directory-wide cumulative.
    Logger logger = mock(Logger.class);
    AtomicLong cumulative = new AtomicLong(500L); // pretend file B already transferred 500

    OssLoggingTransferListener listener = new OssLoggingTransferListener(
        logger, Direction.DOWNLOAD, "fileA", cumulative, true);
    listener.onProgress(100L, 100L, 300L);
    listener.onProgress(200L, 300L, 300L);
    listener.onFinish();

    // Directory cumulative = 500 (pre-existing) + 300 (this file) = 800.
    assertEquals(800L, cumulative.get());

    // The DEBUG completion log records fileBytes=300 and directoryCumulativeBytes=800 as
    // distinct positional args, so operators don't misread the running total as per-file.
    ArgumentCaptor<Object> args = ArgumentCaptor.forClass(Object.class);
    verify(logger).debug(anyString(), args.capture(), args.capture(),
        args.capture(), args.capture(), args.capture());
    List<Object> captured = args.getAllValues();
    // positional: direction, key, fileBytes, directoryCumulativeBytes, elapsed
    assertEquals(300L, captured.get(2), "fileBytes should be this file's own total");
    assertEquals(800L, captured.get(3), "directoryCumulativeBytes should be the shared total");
  }
}
