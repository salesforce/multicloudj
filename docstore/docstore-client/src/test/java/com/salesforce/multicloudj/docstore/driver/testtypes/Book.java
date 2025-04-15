package com.salesforce.multicloudj.docstore.driver.testtypes;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.protobuf.Timestamp;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@AllArgsConstructor
@Data
@NoArgsConstructor
public class Book {
    public String title;
    public Person author;
    public String publisher;
    @JsonSerialize(using = TimestampPbSerializer.class)
    @JsonDeserialize(using = TimestampPbDeserializer.class)
    public Timestamp publishedDate;
    public float price;
    public Map<String, Integer> tableOfContents;
    private String docRevision;
}