package com.salesforce.multicloudj.blob.driver;

import lombok.Getter;

/** The response object returned from completing a multipartUpload */
@Getter
public class MultipartUploadResponse {

  private final String etag;
  private final String checksumValue;

  public MultipartUploadResponse(final String etag) {
    this(etag, null);
  }

  public MultipartUploadResponse(final String etag, final String checksumValue) {
    this.etag = etag;
    this.checksumValue = checksumValue;
  }

}
