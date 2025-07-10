package com.salesforce.multicloudj.docstore.gcp;

import com.google.firestore.v1.Value;
import com.salesforce.multicloudj.docstore.driver.PaginationToken;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * Firestore pagination token implementation.
 * <p>
 * This class stores cursor values for Firestore queries to enable pagination.
 * Firestore uses cursor-based pagination where the cursor contains the field values
 * of the last document in the previous page.
 * If there is an order-by clause, the pagination token must have at-least
 * first of the order-by field in the cursor followed by __name__ (document-id).
 * Otherwise, only document id is used for setting up the cursor value.
 */
public class FSPaginationToken implements PaginationToken {
    
    @Getter
    @Setter
    private List<Value> cursorValues;
    /**
     * Default constructor that creates an empty pagination token.
     */
    public FSPaginationToken() {
        this.cursorValues = null;
    }

    @Override
    public boolean isEmpty() {
        return cursorValues == null || cursorValues.isEmpty();
    }
} 