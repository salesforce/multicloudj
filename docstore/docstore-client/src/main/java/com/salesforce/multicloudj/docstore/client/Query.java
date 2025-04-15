package com.salesforce.multicloudj.docstore.client;

import com.salesforce.multicloudj.common.exceptions.ExceptionHandler;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.docstore.driver.AbstractDocStore;
import com.salesforce.multicloudj.docstore.driver.DocumentIterator;
import com.salesforce.multicloudj.docstore.driver.Filter;
import com.salesforce.multicloudj.docstore.driver.FilterOperation;
import com.salesforce.multicloudj.docstore.driver.Util;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static com.salesforce.multicloudj.docstore.driver.FilterOperation.EQUAL;


@Getter
public class Query {

    private final AbstractDocStore docStore;

    @Setter
    // A list of field path. Each is a dot separated string.
    private List<String> fieldPaths = new ArrayList<>();

    // Filters contain a list of filters for the query. If there are more than one
    // filter, they should be combined with AND.
    private final List<Filter> filters = new ArrayList<>();

    // Offset (also commonly referred to as `Skip`) sets the number of results to skip
    // before returning results. When Offset <= 0, the driver implementation should
    // return all possible results.
    private int offset = 0;

    // Limit sets the maximum number of results returned by running the query. When
    // Limit <= 0, the driver implementation should return all possible results.
    private int limit = 0;

    // OrderByField is the field to use for sorting the results.
    private String orderByField = null;

    // OrderAscending specifies the sort direction.
    private boolean orderAscending = true;

    // BeforeQuery is a callback that must be called exactly once before the
    // underlying service's query is executed. asFunc allows drivers to expose
    // driver-specific types.
    private Consumer<Predicate<Object>> beforeQuery = null;

    public Query(AbstractDocStore docStore) { this.docStore = docStore; }

    // The filters in where clause will be combined with AND
    public Query where(String fieldPath, FilterOperation op, Object value) {
        Util.validateFieldPath(fieldPath);
        if (isInvalidOperation(op, value)) {
            throw new IllegalArgumentException("Invalid filter operation: " + op + " value: " + value);
        }

        filters.add(new Filter(fieldPath, op, value));
        return this;
    }

    private boolean isInvalidOperation(FilterOperation op, Object value) {
        switch (op) {
            case EQUAL:
                return isValidFilterValue(value) && !(value instanceof Boolean);
            case GREATER_THAN:              // Fall through.
            case LESS_THAN:                 // Fall through.
            case GREATER_THAN_OR_EQUAL_TO:  // Fall through.
            case LESS_THAN_OR_EQUAL_TO:
                return isValidFilterValue(value);
            case IN:                        // Fall through.
            case NOT_IN:
                if (!(value instanceof Collection)) {
                    return true;
                }
                for (Object v : (Collection<?>) value) {
                    if (isInvalidOperation(EQUAL, v)) {
                        return true;
                    }
                }
                return false;
            default:
                return true;
        }
    }

    private boolean isValidFilterValue(Object value) {
        return !(value instanceof Number) && !(value instanceof String);
    }

    public Query offset(int n) {
        if (n < 0) {
            throw new IllegalArgumentException("offset must be non-negative.");
        }

        if (this.offset > 0) {
            throw new IllegalArgumentException("query can have at most one offset clause.");
        }

        this.offset = n;
        return this;
    }

    public Query limit(int n) {
        if (n <= 0) {
            throw new IllegalArgumentException("limit must be greater than zero.");
        }

        if (this.limit > 0) {
            throw new IllegalArgumentException("query can have at most one limit clause.");
        }
        this.limit = n;
        return this;
    }

    public Query orderBy(String fieldName, boolean orderAscending) {
        if (fieldName == null) {
            throw new IllegalArgumentException("fieldName must not be null.");
        }

        if (this.orderByField != null) {
            throw new IllegalArgumentException("query can only have one orderBy clause.");
        }

        this.orderByField = fieldName;
        this.orderAscending = orderAscending;
        return this;
    }

    public Query beforeQuery(Consumer<Predicate<Object>> beforeQuery) {
        this.beforeQuery = beforeQuery;
        return this;
    }

    public DocumentIterator get(String... fieldPath) {
        try {
            initGet(List.of(fieldPath));
            return docStore.runGetQuery(this);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = docStore.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
            return null;
        }

    }

    public void initGet(List<String> fieldPaths) {
        docStore.checkClosed();

        for (String fieldPath : fieldPaths) {
            Util.validateFieldPath(fieldPath);
        }

        this.fieldPaths = fieldPaths;
        if (orderByField != null && !filters.isEmpty()) {
            boolean found = false;
            for (Filter filter : filters) {
                String[] paths = filter.getFieldPath().split("\\.");
                if (paths.length == 1 && paths[0].equals(orderByField)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw new IllegalArgumentException(String.format("OrderBy field %s must appear in a Where clause", orderByField));
            }
        }
    }

}
