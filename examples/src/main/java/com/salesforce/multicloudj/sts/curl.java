package com.salesforce.multicloudj.sts;

import com.salesforce.multicloudj.sts.driver.FlowCollector;

import java.net.http.HttpRequest;
import java.nio.ByteBuffer;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class curl {

    public static String requestToCurl(HttpRequest request) {
        String requestBody = "";
        if (request.bodyPublisher().isPresent()) {
            byte[] bytes = request
                    .bodyPublisher()
                    .map(
                            publisher -> {
                                // collect the HttpRequest request body into bytes (if present)
                                FlowCollector<ByteBuffer> collector = new FlowCollector<ByteBuffer>();
                                publisher.subscribe(collector);
                                return collector.items().thenApply(
                                        items ->
                                                items.isEmpty() ? null : items.get(0).array()
                                );
                            })
                    .orElseGet(() -> completedFuture(null)).join();
            requestBody = new String(bytes);
        }

        StringBuilder result = new StringBuilder();

        result.append("curl --location --request ");

        // output method
        result.append(request.method()).append(" ");

        // output url
        result.append("\"")
                .append(request.uri().toString())
                .append("\"");

        // output headers
        request.headers().map().forEach(
                (headerName, headerValue) ->
                        result
                                .append(" -H \"")
                                .append(headerName).append(": ")
                                .append(headerValue.get(0))
                                .append("\"")
        );


        // output body
        if ("post".equalsIgnoreCase(request.method())) {
            if (requestBody.length() > 0) {
                result
                        .append(" -d '")
                        .append(requestBody)
                        .append("'");
            }
        }

        return result.toString();
    }
}
