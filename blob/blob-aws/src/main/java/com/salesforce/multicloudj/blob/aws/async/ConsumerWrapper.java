package com.salesforce.multicloudj.blob.aws.async;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Consumer that wraps another Consumer of a different type, and uses a conversion function
 * to map between source and target types
 * @param <X> the source data type this consumer accepts
 * @param <Y> the target data type this consumer converts to
 */
public class ConsumerWrapper<X, Y> implements Consumer<X> {
    private final Consumer<Y> wrapped;
    private final Function<X, Y> mapper;
    public ConsumerWrapper(Consumer<Y> wrapped, Function<X, Y> mapper) {
        this.wrapped = wrapped;
        this.mapper = mapper;
    }

    /**
     * Accepts the supplied input type X, maps it to the target type Y and invokes the wrapped consumer.
     * @param x the input argument
     */
    @Override
    public void accept(X x) {
        Y targetValue = mapper.apply(x);
        wrapped.accept(targetValue);
    }
}
