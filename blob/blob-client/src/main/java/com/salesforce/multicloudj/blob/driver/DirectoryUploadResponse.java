package com.salesforce.multicloudj.blob.driver;

import java.util.List;
import lombok.Builder;
import lombok.Getter;

/** Wrapper object for directory upload result data */
@Builder
@Getter
public class DirectoryUploadResponse {
  private final List<FailedBlobUpload> failedTransfers;
}
