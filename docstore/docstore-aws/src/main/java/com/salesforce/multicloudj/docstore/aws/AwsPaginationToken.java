package com.salesforce.multicloudj.docstore.aws;

import com.salesforce.multicloudj.docstore.driver.PaginationToken;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class AwsPaginationToken implements PaginationToken {
  @Getter @Setter private Map<String, AttributeValue> exclusiveStartKey = null;

  public AwsPaginationToken() {
    this.exclusiveStartKey = null;
  }

  @Override
  public boolean isEmpty() {
    return exclusiveStartKey == null || exclusiveStartKey.isEmpty();
  }
}
