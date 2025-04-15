package com.salesforce.multicloudj.sts.driver;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;

// Used to extract the body of stdlib HttpRequests
// ref: https://stackoverflow.com/a/77705720
public class FlowCollector<T> implements Flow.Subscriber<T> {
    private final List<T> buffer;
    private final CompletableFuture<List<T>> future;

    public FlowCollector() {
        this.buffer = new ArrayList<>();
        this.future = new CompletableFuture<>();
    }

    public CompletableFuture<List<T>> items() {
        return future;
    }

    @Override
    public void onComplete() {
        future.complete(buffer);
    }

    @Override
    public void onError(Throwable throwable) {
        future.completeExceptionally(throwable);
    }

    @Override
    public void onNext(T item) {
        buffer.add(item);
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        subscription.request(Long.MAX_VALUE);
    }
}