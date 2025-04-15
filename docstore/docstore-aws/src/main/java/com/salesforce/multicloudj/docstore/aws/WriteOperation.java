package com.salesforce.multicloudj.docstore.aws;

import com.salesforce.multicloudj.docstore.driver.Action;
import lombok.AllArgsConstructor;
import lombok.Getter;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;


@AllArgsConstructor
@Getter
public class WriteOperation {
    private Action action;
    private TransactWriteItem writeItem;
    private String newPartitionKey;
    private String newRevision;
    private Runnable run;
}
