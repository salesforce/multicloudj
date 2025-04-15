package com.salesforce.multicloudj.docstore.ali;

import com.alicloud.openservices.tablestore.model.PutRowRequest;
import com.salesforce.multicloudj.docstore.driver.Action;
import lombok.AllArgsConstructor;
import lombok.Getter;


@AllArgsConstructor
@Getter
public class WriteOperation {
    private Action action;
    private PutRowRequest putRowRequest;
    private String newPartitionKey;
    private String newRevision;
    private Runnable run;
}
