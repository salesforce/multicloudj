package com.salesforce.multicloudj.docstore.driver;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

import java.util.List;

@AllArgsConstructor
@Getter
public class Filter {
    private String fieldPath;         // fieldPath is a dot separated string.

    private FilterOperation Op;       // the operation, supports `=`, `>`, `>=`, `<`, `<=`, `in`, `not-in`

    private Object value;             // the value to compare using the operation
}
