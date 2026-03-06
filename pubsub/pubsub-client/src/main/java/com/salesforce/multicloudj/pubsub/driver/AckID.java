package com.salesforce.multicloudj.pubsub.driver;

/**
 * AckID is the identifier of a message for purposes of acknowledgement.
 *
 * <p>This is a marker interface that allows different providers to implement their own
 * acknowledgment ID types while maintaining type safety.
 *
 * <p>Examples of provider-specific implementations:
 *
 * <ul>
 *   <li>AWS SQS: Receipt handle string
 *   <li>GCP Pub/Sub: Ack ID string
 *   <li>Ali MNS: Receipt handle string
 * </ul>
 */
public interface AckID {
  // Marker interface - implementations will contain provider-specific data
}
