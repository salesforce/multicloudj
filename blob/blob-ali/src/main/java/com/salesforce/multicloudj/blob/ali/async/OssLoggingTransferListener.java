package com.salesforce.multicloudj.blob.ali.async;

import com.aliyun.sdk.service.oss2.progress.ProgressListener;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-file transfer progress listener for Ali OSS async directory operations.
 *
 * <p>Implements the OSS v2 SDK's {@link ProgressListener}. For uploads, the SDK drives
 * {@code onProgress}/{@code onFinish} directly (a {@code ProgressObserver} is wired into the
 * upload stream). For downloads the SDK does not drive the listener, so the directory download
 * copy loop invokes {@code onProgress}/{@code onFinish} itself — the same idiom the SDK's own
 * {@code transfermanager.Downloader} uses internally.
 *
 * <p>The {@code onProgress} arguments follow the SDK convention
 * {@code onProgress(increment, cumulativeTransferred, total)}; this listener accumulates the
 * per-call {@code increment} into a directory-scoped shared {@link AtomicLong} so the running
 * total spans every file in the operation.
 *
 * <p>Byte accumulation into the shared counter is always performed (cheap, drives the response's
 * {@code totalBytesTransferred}). Log emission is gated on {@code loggingEnabled}, which is set
 * from the request's {@code transferStatusLoggingEnabled} flag.
 */
final class OssLoggingTransferListener implements ProgressListener {

  private static final Logger DEFAULT_LOGGER =
      LoggerFactory.getLogger(OssLoggingTransferListener.class);

  /** Transfer direction, used only for log context. */
  enum Direction {
    UPLOAD,
    DOWNLOAD
  }

  private final Logger logger;
  private final Direction direction;
  private final String key;
  private final AtomicLong cumulativeTotal;
  private final boolean loggingEnabled;
  private final Instant startTime;
  // Per-file byte count for this listener instance, distinct from the directory-wide
  // cumulativeTotal. Single-writer per instance (downloads: our copy loop; uploads: the
  // SDK's per-stream observer), so a plain long is sufficient.
  private long bytesThisFile;

  static OssLoggingTransferListener create(
      Direction direction, String key, AtomicLong cumulativeTotal, boolean loggingEnabled) {
    return new OssLoggingTransferListener(
        DEFAULT_LOGGER, direction, key, cumulativeTotal, loggingEnabled);
  }

  // Package-private: production constructs via create() with the default logger; the
  // same-package unit test constructs directly with a logger to verify emission/gating.
  OssLoggingTransferListener(
      Logger logger, Direction direction, String key,
      AtomicLong cumulativeTotal, boolean loggingEnabled) {
    this.logger = logger;
    this.direction = direction;
    this.key = key;
    this.cumulativeTotal = cumulativeTotal;
    this.loggingEnabled = loggingEnabled;
    // Instant.now() is timezone-agnostic; used only for elapsed-duration calculation.
    this.startTime = Instant.now();
    if (loggingEnabled) {
      logger.debug(
          "substrate SDK transferListener: direction={}, key={}. transfer initiated.",
          direction, key);
    }
  }

  @Override
  public void onProgress(long increment, long transferred, long total) {
    bytesThisFile += increment;
    cumulativeTotal.addAndGet(increment);
    if (loggingEnabled) {
      logger.trace(
          "substrate SDK transferListener: direction={}, key={}, increment={},"
              + " transferred={}, total={}. bytes transferred.",
          direction, key, increment, transferred, total);
    }
  }

  @Override
  public void onFinish() {
    if (loggingEnabled) {
      Duration elapsed = Duration.between(startTime, Instant.now());
      logger.info(
          "substrate SDK transferListener: direction={}, key={}, fileBytes={},"
              + " directoryCumulativeBytes={}, elapsed={}. transfer complete.",
          direction, key, bytesThisFile, cumulativeTotal.get(), elapsed);
    }
  }

  /**
   * Records a failed transfer. Mirrors AWS's gated {@code transferFailed}: emitted only when
   * logging is enabled. The response's {@code failedTransfers} list remains the always-on failure
   * surface and is populated independently by the directory operation.
   */
  void transferFailed(Throwable ex) {
    if (loggingEnabled) {
      Duration elapsed = Duration.between(startTime, Instant.now());
      logger.error(
          "substrate SDK transferListener: direction={}, key={}, fileBytes={},"
              + " directoryCumulativeBytes={}, elapsed={}. transfer failed.",
          direction, key, bytesThisFile, cumulativeTotal.get(), elapsed, ex);
    }
  }
}
