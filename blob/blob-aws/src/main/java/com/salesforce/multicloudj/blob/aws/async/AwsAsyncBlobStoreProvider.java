package com.salesforce.multicloudj.blob.aws.async;

import com.google.auto.service.AutoService;
import com.salesforce.multicloudj.blob.async.driver.AsyncBlobStoreProvider;
import com.salesforce.multicloudj.common.aws.AwsConstants;

@AutoService(AsyncBlobStoreProvider.class)
public class AwsAsyncBlobStoreProvider implements AsyncBlobStoreProvider {

    @Override
    public AwsAsyncBlobStore.Builder builder() {
        return AwsAsyncBlobStore.builder();
    }

    @Override
    public String getProviderId() {
        return AwsConstants.PROVIDER_ID;
    }
}
