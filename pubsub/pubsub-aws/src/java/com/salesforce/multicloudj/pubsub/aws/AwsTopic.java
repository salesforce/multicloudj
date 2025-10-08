package com.salesforce.multicloudj.pubsub.aws;

import com.salesforce.multicloudj.common.aws.AwsConstants;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.pubsub.driver.AbstractTopic;
import com.salesforce.multicloudj.pubsub.driver.Message;

import java.util.List;

public class AwsTopic extends AbstractTopic<AwsTopic> {
    
    public AwsTopic() {
        this(new Builder());
    }
    
    public AwsTopic(Builder builder) {
        super(builder);
    }

    @Override
    protected void doSendBatch(List<Message> messages) {
        // TODO: Will create the aws implementation for batch sending later
    }

    @Override
    public Class<? extends SubstrateSdkException> getException(Throwable t) {
        return null;
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
    
    public Builder builder() {
        return new Builder();
    }
    
    public static class Builder extends AbstractTopic.Builder<AwsTopic> {
        
        public Builder() {
            this.providerId = AwsConstants.PROVIDER_ID;
        }
        
        @Override
        public AwsTopic build() {
            return new AwsTopic(this);
        }
    }
} 