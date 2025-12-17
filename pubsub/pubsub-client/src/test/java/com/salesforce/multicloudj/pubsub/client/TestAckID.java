package com.salesforce.multicloudj.pubsub.client;

import com.salesforce.multicloudj.pubsub.driver.AckID;

public class TestAckID implements AckID {
    private final String id;

    public TestAckID(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    @Override
    public String toString() {
        return id; 
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TestAckID testAckID = (TestAckID) o;
        return id.equals(testAckID.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }
} 