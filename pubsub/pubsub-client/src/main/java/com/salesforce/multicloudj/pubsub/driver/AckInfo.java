package com.salesforce.multicloudj.pubsub.driver;

/**
 * Represents an acknowledgment or negative acknowledgment for a message.
 */
public class AckInfo {
    private final String ackID;
    private final boolean isAck;

    public AckInfo(String ackID, boolean isAck) {
        this.ackID = ackID;
        this.isAck = isAck;
    }

    public String getAckID() {
        return ackID;
    }

    public boolean isAck() {
        return isAck;
    }
}

