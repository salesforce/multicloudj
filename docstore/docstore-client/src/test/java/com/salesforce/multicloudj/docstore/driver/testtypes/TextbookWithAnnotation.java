package com.salesforce.multicloudj.docstore.driver.testtypes;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TextbookWithAnnotation extends BookWithAnnotation {
    @JsonProperty("Subject")
    private String subject;
    @JsonProperty("Grade")
    private int grade;
}