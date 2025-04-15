package com.salesforce.multicloudj.docstore.driver.testtypes;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

public class BookWithAnnotation {
    @JsonProperty("Title")
    private String title;
    @JsonProperty("Author")
    private Person author;
    @JsonProperty("Publisher")
    private String publisher;
    @JsonIgnore
    private Date publishedDate;
    @JsonIgnore
    private float price;
}