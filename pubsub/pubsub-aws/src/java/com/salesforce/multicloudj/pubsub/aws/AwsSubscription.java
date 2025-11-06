package com.salesforce.multicloudj.pubsub.aws;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.salesforce.multicloudj.common.aws.AwsConstants;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.pubsub.batcher.Batcher;
import com.salesforce.multicloudj.pubsub.client.GetAttributeResult;
import com.salesforce.multicloudj.pubsub.driver.AbstractSubscription;
import com.salesforce.multicloudj.pubsub.driver.AckID;
import com.salesforce.multicloudj.pubsub.driver.Message;

public class AwsSubscription extends AbstractSubscription<AwsSubscription> {
    
    public AwsSubscription() {
        this(new Builder());
    }
    
    public AwsSubscription(Builder builder) {
        super(builder);
    }

    @Override
    public void sendAck(AckID ackID) {
    }

    @Override
    public CompletableFuture<Void> sendAcks(List<AckID> ackIDs) {
        return null;
    }

    @Override
    protected void doSendAcks(List<AckID> ackIDs) {
        // TODO: Implement AWS SQS acknowledgment
    }

    @Override
    protected void doSendNacks(List<AckID> ackIDs) {
        // TODO: Implement AWS SQS negative acknowledgment
    }

    @Override
    public void sendNack(AckID ackID) {
    }

    @Override
    public CompletableFuture<Void> sendNacks(List<AckID> ackIDs) {
        return null;
    }

    @Override
    public boolean canNack() {
        return false;
    }

    @Override
    public GetAttributeResult getAttributes() {
        return new GetAttributeResult.Builder()
                .name("aws-subscription")
                .topic("aws-topic")
                .build();
    }

    @Override
    public boolean isRetryable(Throwable error) {
        return false;
    }

    @Override
    public Class<? extends SubstrateSdkException> getException(Throwable t) {
        return null;
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    protected List<Message> doReceiveBatch(int batchSize) {
        return Collections.emptyList();
    }
    
    @Override
    protected Batcher.Options createAckBatcherOptions() {
        // AWS implementation not yet complete
        throw new UnsupportedOperationException("AWS PubSub implementation not yet complete");
    }

    public Builder builder() {
        return new Builder();
    }
    
    public static class Builder extends AbstractSubscription.Builder<AwsSubscription> {
        
        public Builder() {
            this.providerId = AwsConstants.PROVIDER_ID;
        }
        
        @Override
        public AwsSubscription build() {
            return new AwsSubscription(this);
        }
    }
} 