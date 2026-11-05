package com.salesforce.multicloudj.docstore.driver;

import lombok.Builder;
import lombok.Value;

/** Options for one read operation, with unset values inherited from the client configuration. */
@Value
@Builder
public class ReadOptions {
  /**
   * True requires strong consistency; false uses the substrate's normal read behavior. Null
   * inherits the client setting. Strong consistency does not imply an atomic multi-document
   * snapshot.
   */
  Boolean consistentRead;
}
