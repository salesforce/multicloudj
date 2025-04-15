package com.salesforce.multicloudj.docstore.driver.testtypes;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.protobuf.Timestamp;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Collection;

@AllArgsConstructor
@Data
@NoArgsConstructor
public class Person {
    public Collection<String> nickNames;
    public String firstName;
    public String lastName;
    @JsonSerialize(using = TimestampPbSerializer.class)
    @JsonDeserialize(using = TimestampPbDeserializer.class)
    public Timestamp dateOfBirth;
}
