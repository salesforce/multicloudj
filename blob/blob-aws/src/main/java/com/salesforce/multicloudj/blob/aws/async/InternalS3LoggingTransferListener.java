package com.salesforce.multicloudj.blob.aws.async;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.transfer.s3.progress.TransferListener;

final class InternalS3LoggingTransferListener implements TransferListener {
  private static final Logger logger =
      LoggerFactory.getLogger(InternalS3LoggingTransferListener.class);

  private final AtomicLong totalBytesTransferred;
  private Instant startTime;

  InternalS3LoggingTransferListener(AtomicLong totalBytesTransferred) {
    this.totalBytesTransferred = totalBytesTransferred;
  }

  @Override
  public void transferInitiated(Context.TransferInitiated context) {
    startTime = Instant.now();
    logger.debug("context={}; Transfer initiated", context);
  }

  @Override
  public void bytesTransferred(Context.BytesTransferred context) {
    logger.trace("context={}; bytesTransferred()", context);
  }

  @Override
  public void transferComplete(Context.TransferComplete context) {
    long transferred = context.progressSnapshot().transferredBytes();
    long oldValue = totalBytesTransferred.getAndAdd(transferred);
    logger.debug(
        "context={}; oldValue={}; newValue={}; transferComplete() collecting transferred bytes",
        context,
        oldValue,
        totalBytesTransferred.get());
    Duration transferTime =
        startTime == null ? Duration.ZERO : Duration.between(startTime, Instant.now());
    logger.debug(
        "transferTime={}; transferTimeNs={}; progressSnapshot={}; Transfer complete",
        transferTime,
        transferTime.toNanos(),
        context.progressSnapshot());
  }

  @Override
  public void transferFailed(Context.TransferFailed context) {
    long transferred = context.progressSnapshot().transferredBytes();
    long oldValue = totalBytesTransferred.getAndAdd(transferred);
    logger.debug(
        "context={}; oldValue={}; newValue={}; transferFailed() collecting transferred bytes",
        context,
        oldValue,
        totalBytesTransferred.get());
    Duration transferTime =
        startTime == null ? Duration.ZERO : Duration.between(startTime, Instant.now());
    logger.warn(
        "transferTime={}; transferTimeNs={}; progressSnapshot={}; Transfer failed",
        transferTime,
        transferTime.toNanos(),
        context.progressSnapshot(),
        context.exception());
  }
}
