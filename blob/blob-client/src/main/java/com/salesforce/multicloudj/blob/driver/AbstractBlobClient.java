package com.salesforce.multicloudj.blob.driver;

import com.salesforce.multicloudj.common.provider.Provider;
import com.salesforce.multicloudj.common.service.SdkService;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import lombok.Getter;

/**
 * Base class for substrate-specific implementations.
 * This class serves the purpose of providing common (i.e. substrate-agnostic) functionality.
 */
public abstract class AbstractBlobClient<T extends AbstractBlobClient<T>> implements Provider, SdkService {

    private final String providerId;
    @Getter
    protected final String region;

    protected final CredentialsOverrider credentialsOverrider;

    protected AbstractBlobClient(Builder<T> builder) {
        this(
                builder.getProviderId(),
                builder.getRegion(),
                builder.getCredentialsOverrider()
        );
    }

    public AbstractBlobClient(
            String providerId,
            String region,
            CredentialsOverrider credentials
    ){
        this.providerId = providerId;
        this.region = region;
        this.credentialsOverrider = credentials;
    }

    @Override
    public String getProviderId() {
        return providerId;
    }

    /**
     * Passes the call to the substrate-specific listBuckets methods
     *
     */
    public ListBucketsResponse listBuckets() {
        return doListBuckets();
    }

    protected abstract ListBucketsResponse doListBuckets();

    public abstract static class Builder<T extends AbstractBlobClient<T>>
            extends BlobBuilder<T>
            implements Provider.Builder {

        @Override
        public Builder<T> providerId(String providerId) {
            super.providerId(providerId);
            return this;
        }
    }
}
