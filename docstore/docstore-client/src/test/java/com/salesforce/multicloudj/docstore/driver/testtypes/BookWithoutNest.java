package com.salesforce.multicloudj.docstore.driver.testtypes;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.protobuf.Timestamp;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@Data
@NoArgsConstructor
public class BookWithoutNest {
    public String title;
    public String author;
    public String publisher;
    @JsonSerialize(using = TimestampPbSerializer.class)
    @JsonDeserialize(using = TimestampPbDeserializer.class)
    public Timestamp publishedDate;
    public float price;
    String docRevision;
}