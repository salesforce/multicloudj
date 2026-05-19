package com.salesforce.multicloudj.blob.async.driver;

import com.salesforce.multicloudj.blob.driver.BlobStoreBuilder;
import com.salesforce.multicloudj.common.provider.SdkProvider;

public interface AsyncBlobStoreProvider extends SdkProvider<AsyncBlobStore> {

  abstract class Builder extends BlobStoreBuilder<AsyncBlobStore>
      implements SdkProvider.Builder<AsyncBlobStore> {
    private Boolean useTransferListener;

    public Boolean getUseTransferListener() {
      return useTransferListener;
    }

    public Builder withUseTransferListener(Boolean useTransferListener) {
      this.useTransferListener = useTransferListener;
      return this;
    }
  }

  @Override
  Builder builder();
}
