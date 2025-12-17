package com.salesforce.multicloudj.blob.gcp.async;

import com.google.auto.service.AutoService;
import com.salesforce.multicloudj.blob.async.driver.AsyncBlobStoreProvider;
import com.salesforce.multicloudj.common.gcp.GcpConstants;

@AutoService(AsyncBlobStoreProvider.class)
public class GcpAsyncBlobStoreProvider implements AsyncBlobStoreProvider {

    @Override
    public GcpAsyncBlobStore.Builder builder() {
        return GcpAsyncBlobStore.builder();
    }

    @Override
    public String getProviderId() {
        return GcpConstants.PROVIDER_ID;
    }
}
