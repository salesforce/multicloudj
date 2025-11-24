package com.salesforce.multicloudj.pubsub.driver;

/**
 * Represents an acknowledgment or negative acknowledgment for a message.
 */
public class AckInfo {
    private final AckID ackID;
    private final boolean isAck;

    public AckInfo(AckID ackID, boolean isAck) {
        this.ackID = ackID;
        this.isAck = isAck;
    }

    public AckID getAckID() {
        return ackID;
    }

    public boolean isAck() {
        return isAck;
    }
}

