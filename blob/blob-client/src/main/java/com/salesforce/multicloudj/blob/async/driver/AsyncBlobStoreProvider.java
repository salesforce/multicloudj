package com.salesforce.multicloudj.blob.async.driver;

import com.salesforce.multicloudj.blob.driver.BlobStoreBuilder;
import com.salesforce.multicloudj.common.provider.SdkProvider;
import lombok.Getter;

import java.util.concurrent.ExecutorService;

public interface AsyncBlobStoreProvider extends SdkProvider<AsyncBlobStore> {

    abstract class Builder
            extends BlobStoreBuilder<AsyncBlobStore>
            implements SdkProvider.Builder<AsyncBlobStore> {

        @Getter
        private ExecutorService executorService;

        /**
         * Allows providing a custom ExecutorService to be used by the async client.
         * If this value is not set (or is set to null), then the async client will
         * use its own default executor.
         * @param executorService The ExecutorService to use
         * @return Returns this Builder
         */
        public Builder withExecutorService(ExecutorService executorService) {
            this.executorService = executorService;
            return this;
        }
    }

    @Override
    Builder builder();
}
