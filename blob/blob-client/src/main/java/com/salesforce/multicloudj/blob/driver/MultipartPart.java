package com.salesforce.multicloudj.blob.driver;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import lombok.Getter;

/** One of the individual "parts" of a multipartUpload */
@Getter
public class MultipartPart {

  private final int partNumber;
  private final InputStream inputStream;
  private final long contentLength;
  private final String checksumValue;

  public MultipartPart(final int partNumber, final byte[] content) {
    this(partNumber, new ByteArrayInputStream(content), content.length, null);
  }

  public MultipartPart(final int partNumber, final InputStream inputStream,
                       final long contentLength) {
    this(partNumber, inputStream, contentLength, null);
  }

  public MultipartPart(final int partNumber, final byte[] content, final String checksumValue) {
    this(partNumber, new ByteArrayInputStream(content), content.length, checksumValue);
  }

  public MultipartPart(final int partNumber, final InputStream inputStream,
                       final long contentLength, final String checksumValue) {
    this.partNumber = partNumber;
    this.inputStream = inputStream;
    this.contentLength = contentLength;
    this.checksumValue = checksumValue;
  }

}
