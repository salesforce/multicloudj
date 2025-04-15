package com.salesforce.multicloudj.docstore.driver;

public interface DocumentIterator {
    void next(Document document);
    boolean hasNext();
    void stop();
}
