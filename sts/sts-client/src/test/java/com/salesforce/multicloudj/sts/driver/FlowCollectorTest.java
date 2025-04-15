package com.salesforce.multicloudj.sts.driver;


import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpRequest;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FlowCollectorTest {

    String region = "us-west-2";
    String service = "ec2";

    @Test
    void testEmptyBody() {
        // create a sample bodypublisher
        HttpRequest.BodyPublisher body = HttpRequest.BodyPublishers.noBody();

        // the request object we want to sign
        HttpRequest fakeRequest = HttpRequest.newBuilder()
                .POST(body)
                .uri(URI.create("https://" + service + "." + region + ".amazonaws.com:443"))
                .build();

        FlowCollector<ByteBuffer> collector = new FlowCollector<ByteBuffer>();
        fakeRequest.bodyPublisher().ifPresent(publisher -> publisher.subscribe(collector));
        assertEquals(0, collector.items().join().size());
    }

    @Test
    void testNonEmptyBody() {
        String apiName = "GetCallerIdentity";
        String apiVersion = "2011-06-15";
        String bodyValue = "Action=" + apiName + "&Version=" + apiVersion;

        // create a sample bodypublisher
        HttpRequest.BodyPublisher body = HttpRequest.BodyPublishers.ofString(bodyValue);

        // the request object we want to sign
        HttpRequest fakeRequest = HttpRequest.newBuilder()
                .POST(body)
                .uri(URI.create("https://" + service + "." + region + ".amazonaws.com:443"))
                .build();

        FlowCollector<ByteBuffer> collector = new FlowCollector<ByteBuffer>();
        fakeRequest.bodyPublisher().ifPresent(publisher -> publisher.subscribe(collector));
        assertEquals(1, collector.items().join().size());

        ByteBuffer byteBuffer = collector.items().join().get(0);
        byte[] bytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(bytes);
        assertEquals(bodyValue, new String(bytes));

    }

}