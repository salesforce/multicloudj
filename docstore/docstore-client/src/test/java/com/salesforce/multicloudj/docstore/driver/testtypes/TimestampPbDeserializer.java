package com.salesforce.multicloudj.docstore.driver.testtypes;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.google.protobuf.Timestamp;

import java.io.IOException;
import java.time.Instant;

public class TimestampPbDeserializer extends JsonDeserializer<Timestamp> {
    @Override
    public Timestamp deserialize(JsonParser parser, DeserializationContext context) throws IOException {
        // Parse the ISO-8601 formatted date string
        String dateString = parser.getText();

        // Convert the date string to an Instant
        Instant instant = Instant.parse(dateString);

        // Build a Protobuf Timestamp from the Instant
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }
}