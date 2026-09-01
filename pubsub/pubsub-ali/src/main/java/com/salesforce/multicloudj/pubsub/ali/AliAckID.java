package com.salesforce.multicloudj.pubsub.ali;

import com.salesforce.multicloudj.pubsub.driver.AckID;
import java.util.Objects;

/**
 * Alibaba SMQ (MNS) implementation of {@link AckID}.
 *
 * <p>Wraps the receipt handle returned by SMQ when a message is received. The handle is required to
 * delete (ack) or change the visibility timeout of (nack) a message.
 */
public class AliAckID implements AckID {

  private final String receiptHandle;

  public AliAckID(String receiptHandle) {
    if (receiptHandle == null || receiptHandle.trim().isEmpty()) {
      throw new IllegalArgumentException("Receipt handle cannot be null or empty");
    }
    this.receiptHandle = receiptHandle;
  }

  public String getReceiptHandle() {
    return receiptHandle;
  }

  @Override
  public String toString() {
    return receiptHandle;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AliAckID)) {
      return false;
    }
    return receiptHandle.equals(((AliAckID) o).receiptHandle);
  }

  @Override
  public int hashCode() {
    return Objects.hash(receiptHandle);
  }
}
