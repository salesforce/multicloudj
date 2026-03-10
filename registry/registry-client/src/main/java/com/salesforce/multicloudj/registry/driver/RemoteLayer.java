package com.salesforce.multicloudj.registry.driver;

import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.registry.model.Layer;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

/** Layer for a remote registry; blob is fetched and decompressed (gzip) on demand. */
final class RemoteLayer implements Layer {

  private final OciRegistryClient client;
  private final String repository;
  private final String digest;

  RemoteLayer(OciRegistryClient client, String repository, String digest) {
    this.client = client;
    this.repository = repository;
    this.digest = digest;
  }

  @Override
  public String getDigest() {
    return digest;
  }

  @Override
  public InputStream getUncompressed() {
    InputStream compressed = null;
    try {
      compressed = client.downloadBlob(repository, digest);
      return new GzipCompressorInputStream(compressed);
    } catch (IOException e) {
      if (compressed != null) {
        try {
          compressed.close();
        } catch (IOException suppressed) {
          e.addSuppressed(suppressed);
        }
      }
      throw new UnknownException("Failed to decompress layer", e);
    }
  }

  @Override
  public long getSize() {
    return -1;
  }
}
