package com.salesforce.multicloudj.blob.driver;

import lombok.Getter;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

/**
 * One of the individual "parts" of a multipartUpload
 */
@Getter
public class MultipartPart {

    private final int partNumber;
    private final InputStream inputStream;
    private final long contentLength;

    public MultipartPart(final int partNumber, final byte[] content){
        this(partNumber, new ByteArrayInputStream(content), content.length);
    }

    public MultipartPart(final int partNumber, final InputStream inputStream, final long contentLength){
        this.partNumber = partNumber;
        this.inputStream = inputStream;
        this.contentLength = contentLength;
    }

}
