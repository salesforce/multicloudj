package com.salesforce.multicloudj.docstore.ali;

import com.alicloud.openservices.tablestore.model.RowChange;
import com.salesforce.multicloudj.docstore.driver.Action;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class WriteOperation {
  private Action action;
  // The native Tablestore change (RowPutChange or RowDeleteChange). The transactional path
  // (runTxWrites) branches on its concrete type to issue the correct putRow/deleteRow call; the
  // non-atomic path executes via the run runnable instead.
  private RowChange rowChange;
  private String newPartitionKey;
  private String newRevision;
  private Runnable run;
}
