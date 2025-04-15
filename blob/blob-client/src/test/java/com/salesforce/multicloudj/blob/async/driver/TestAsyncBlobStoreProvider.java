package com.salesforce.multicloudj.blob.async.driver;

public class TestAsyncBlobStoreProvider implements AsyncBlobStoreProvider {

    @Override
    public Builder builder() {
        return TestAsyncBlobStore.builder();
    }

    @Override
    public String getProviderId() {
        return TestAsyncBlobStore.PROVIDER_ID;
    }
}
