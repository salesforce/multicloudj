package com.salesforce.multicloudj.blob.aws.async;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.TransferRequest;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;
import software.amazon.awssdk.transfer.s3.progress.TransferListener;
import software.amazon.awssdk.transfer.s3.progress.TransferProgressSnapshot;

public final class S3LoggingTransferListener implements TransferListener {
  private static final Logger logger = LoggerFactory.getLogger(S3LoggingTransferListener.class);

  private final AtomicLong totalBytesTransferred;
  private Instant startTime;

  public static S3LoggingTransferListener create(AtomicLong totalBytesTransferred) {
    return new S3LoggingTransferListener(totalBytesTransferred);
  }

  S3LoggingTransferListener(AtomicLong totalBytesTransferred) {
    this.totalBytesTransferred = totalBytesTransferred;
  }

  @Override
  public void transferInitiated(Context.TransferInitiated context) {
    // Instant.now() is timezone-agnostic; used only for elapsed duration calculation.
    startTime = Instant.now();
    TransferProgressSnapshot snapshot = context.progressSnapshot();
    logger.debug(
        "substrate SDK transferListener: key={}, bucket={},"
            + " transferredBytes={}, totalBytes={}. transfer initiated.",
        getKey(context.request()),
        getBucket(context.request()),
        snapshot.transferredBytes(),
        snapshot.totalBytes().isPresent() ? snapshot.totalBytes().getAsLong() : "unknown");
  }

  @Override
  public void bytesTransferred(Context.BytesTransferred context) {
    logger.trace("substrate SDK transferListener: context={}; bytesTransferred()", context);
  }

  @Override
  public void transferComplete(Context.TransferComplete context) {
    long transferred = context.progressSnapshot().transferredBytes();
    long oldValue = totalBytesTransferred.getAndAdd(transferred);
    Duration transferTime =
        startTime == null ? Duration.ZERO : Duration.between(startTime, Instant.now());
    logger.info(
        "substrate SDK transferListener: oldValue={}, newValue={},"
            + " transferTime={}, progressSnapshot={}. transfer complete.",
        oldValue,
        totalBytesTransferred.get(),
        transferTime,
        context.progressSnapshot());
  }

  @Override
  public void transferFailed(Context.TransferFailed context) {
    long transferred = context.progressSnapshot().transferredBytes();
    long oldValue = totalBytesTransferred.getAndAdd(transferred);
    Duration transferTime =
        startTime == null ? Duration.ZERO : Duration.between(startTime, Instant.now());
    logger.error(
        "substrate SDK transferListener: oldValue={}, newValue={},"
            + " transferTime={}, progressSnapshot={}. transfer failed.",
        oldValue,
        totalBytesTransferred.get(),
        transferTime,
        context.progressSnapshot(),
        context.exception());
  }

  private static String getKey(TransferRequest request) {
    if (request instanceof UploadFileRequest) {
      return ((UploadFileRequest) request).putObjectRequest().key();
    } else if (request instanceof DownloadFileRequest) {
      return ((DownloadFileRequest) request).getObjectRequest().key();
    }
    return "";
  }

  private static String getBucket(TransferRequest request) {
    if (request instanceof UploadFileRequest) {
      return ((UploadFileRequest) request).putObjectRequest().bucket();
    } else if (request instanceof DownloadFileRequest) {
      return ((DownloadFileRequest) request).getObjectRequest().bucket();
    }
    return "";
  }
}
