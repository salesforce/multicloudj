package com.salesforce.multicloudj.blob.async.driver;

import com.salesforce.multicloudj.blob.driver.BlobStoreBuilder;
import com.salesforce.multicloudj.common.provider.SdkProvider;

public interface AsyncBlobStoreProvider extends SdkProvider<AsyncBlobStore> {

    abstract class Builder
            extends BlobStoreBuilder<AsyncBlobStore>
            implements SdkProvider.Builder<AsyncBlobStore> {}

    @Override
    Builder builder();
}
