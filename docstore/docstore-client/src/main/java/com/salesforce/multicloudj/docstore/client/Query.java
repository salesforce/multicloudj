package com.salesforce.multicloudj.docstore.client;

import com.salesforce.multicloudj.common.exceptions.ExceptionHandler;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.docstore.driver.AbstractDocStore;
import com.salesforce.multicloudj.docstore.driver.DocumentIterator;
import com.salesforce.multicloudj.docstore.driver.Filter;
import com.salesforce.multicloudj.docstore.driver.FilterOperation;
import com.salesforce.multicloudj.docstore.driver.Util;
import com.salesforce.multicloudj.docstore.driver.PaginationToken;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static com.salesforce.multicloudj.docstore.driver.FilterOperation.EQUAL;

/**
 * Query provides a fluent interface for building and executing queries against document stores.
 * 
 * <p>This class allows you to construct complex queries with filtering, sorting, pagination,
 * and field selection capabilities across multiple cloud providers including AWS DynamoDB,
 * GCP Firestore, and Alibaba TableStore.
 * 
 * <p>The Query class supports:
 * <ul>
 *   <li>Filtering with various operations (EQUAL, GREATER_THAN, LESS_THAN, IN, etc.)</li>
 *   <li>Sorting by field with ascending/descending order</li>
 *   <li>Pagination with offset and limit</li>
 *   <li>Field projection to retrieve only specific fields</li>
 *   <li>Pre-query callbacks for custom processing, not supported yet</li>
 * </ul>
 * 
 * <p>Usage example:
 * <pre>
 * DocStoreClient client = DocStoreClient.builder("aws").build();
 * 
 * // Simple query with filtering
 * DocumentIterator results = client.query()
 *     .where("age", FilterOperation.GREATER_THAN, 25)
 *     .where("status", FilterOperation.EQUAL, "active")
 *     .limit(10)
 *     .orderBy("name", true)
 *     .get();
 * 
 * // Process results
 *       Person p = new Person();
 *       while (iter.hasNext()) {
 *           iter.next(new Document(p));
 *           System.out.println(p);
 *       }
 * </pre>
 * 
 * <p><strong>Note:</strong> Multiple filters are combined using AND logic.
 * When using orderBy, the order field must appear in at least one where clause.
 * 
 * @since 0.1.0
 */
@Getter
public class Query {

    /** The underlying document store implementation used to execute queries. */
    private final AbstractDocStore docStore;

    /**
     * A list of field paths to retrieve. Each field path is a dot-separated string (e.g., "user.name", "address.city").
     * */
    @Setter
    private List<String> fieldPaths = new ArrayList<>();

    /** 
     * Filters contain a list of filters for the query. If there are more than one
     * filter, they are combined with AND logic.
     */
    private final List<Filter> filters = new ArrayList<>();

    /** 
     * Offset (also commonly referred to as `Skip`) sets the number of results to skip
     * before returning results. When offset <= 0, the driver implementation returns
     * all possible results from the beginning.
     */
    private int offset = 0;

    /** 
     * Limit sets the maximum number of results returned by running the query. When
     * limit <= 0, the driver implementation returns all possible results.
     */
    private int limit = 0;

    /**
     * paginationToken sets the pagination token returned by the previous query.
     * Pagination token being empty guarantees that there are no more documents left.
     * However, pagination token being non-empty doesn't guarantee that there
     * are documents left. For example: firestore pagination is stateless cursor values
     * and can only be inferred from the last returned document unless dynamoDB which
     * returns the null lastEvaluatedKey if no more data left.
     */
    private PaginationToken paginationToken = null;

    /**
     * The field to use for sorting the results. Must appear in at least one where clause.
     * If there is a range comparison, it must have the order by clause to work
     * without issues.
     * see: <a href="https://firebase.google.com/docs/firestore/query-data/order-limit-data?limitations#limitations">
     * </a>
     * The order by field also must appear in the cursor
     * <a href="https://firebase.google.com/docs/firestore/reference/rest/v1beta1/StructuredQuery">...</a>
     */
    private String orderByField = null;

    /** Specifies the sort direction. True for ascending, false for descending. */
    private boolean orderAscending = true;

    /** 
     * BeforeQuery is a callback that is called exactly once before the
     * underlying service's query is executed. This allows drivers to expose
     * driver-specific functionality.
     */
    private Consumer<Predicate<Object>> beforeQuery = null;

    /**
     * Creates a new Query instance for the specified document store.
     * 
     * @param docStore the document store implementation to query against
     */
    public Query(AbstractDocStore docStore) { this.docStore = docStore; }

    /**
     * Adds a filter condition to the query. Multiple where clauses are combined with AND logic.
     * 
     * <p>Supported operations include:
     * <ul>
     *   <li>EQUAL - exact match for strings, numbers</li>
     *   <li>GREATER_THAN, LESS_THAN - comparison for numbers and strings</li>
     *   <li>GREATER_THAN_OR_EQUAL_TO, LESS_THAN_OR_EQUAL_TO - inclusive comparisons</li>
     *   <li>IN, NOT_IN - membership testing with collections</li>
     * </ul>
     * 
     * <p>Field paths can use dot notation to access nested fields (e.g., "user.profile.age").
     * 
     * @param fieldPath the field path to filter on, supports dot notation for nested fields
     * @param op the filter operation to apply
     * @param value the value to compare against. For IN/NOT_IN operations, should be a Collection
     * @return this Query instance for method chaining
     * @throws IllegalArgumentException if fieldPath is invalid, operation is unsupported, 
     *                                  or value type is incompatible with the operation
     */
    public Query where(String fieldPath, FilterOperation op, Object value) {
        Util.validateFieldPath(fieldPath);
        if (isInvalidOperation(op, value)) {
            throw new IllegalArgumentException("Invalid filter operation: " + op + " value: " + value);
        }

        filters.add(new Filter(fieldPath, op, value));
        return this;
    }

    /**
     * Validates whether the given operation and value combination is valid.
     * 
     * @param op the filter operation to validate
     * @param value the value to validate against the operation
     * @return true if the operation is invalid, false if valid
     */
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

    /**
     * Checks if the filter value is of a supported type.
     * Supported types are Number and String.
     * 
     * @param value the value to validate
     * @return true if the value type is valid, false otherwise
     */
    private boolean isValidFilterValue(Object value) {
        return !(value instanceof Number) && !(value instanceof String);
    }

    /**
     * Sets the number of results to skip before returning results.
     * This is useful for pagination when combined with limit().
     * 
     * <p>When offset is 0 or negative, no results are skipped.
     * Only one offset clause is allowed per query.
     * 
     * @param n the number of results to skip, must be non-negative
     * @return this Query instance for method chaining
     */
    public Query offset(int n) {
        if (n < 0) {
            throw new IllegalArgumentException("offset must be non-negative.");
        }

        if (this.offset > 0) {
            throw new IllegalArgumentException("query can have at most one offset clause.");
        }

        if (paginationToken != null) {
            throw new IllegalArgumentException("query already has one pagination token.");
        }

        this.offset = n;
        return this;
    }

    /**
     * Sets the maximum number of results to return.
     * This is useful for pagination and performance optimization.
     * 
     * <p>When limit is 0 or negative, all possible results are returned.
     * Only one limit clause is allowed per query.
     * 
     * @param n the maximum number of results to return, must be greater than zero
     * @return this Query instance for method chaining
     */
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

    /**
     * Sets the field to use pagination query.
     *
     * Use pagination token will make the query start directly from the result of the last query.
     *
     * @param paginationToken the pagination token that was returned from the last query.
     */
    public Query paginationToken(PaginationToken paginationToken) {
        if (offset > 0) {
            throw new IllegalArgumentException("query already has set an offset.");
        }

        this.paginationToken = paginationToken;
        return this;
    }

    /**
     * Sets the field to use for sorting the query results.
     *
     * <p><strong>Important:</strong> The orderBy field must appear in at least one where clause
     * to ensure efficient query execution across different cloud providers.
     * Only one orderBy clause is allowed per query. If there are in-equality
     * filters, the order-by is advisable to have the consistent results across substrates
     * because of Google cloud firestore limitations.
     * <a href="https://firebase.google.com/docs/firestore/query-data/order-limit-data?limitations#limitations">...</a>
     *
     * @param fieldName      the field name to sort by, must not be null
     * @param orderAscending true for ascending order, false for descending order
     * @return this Query instance for method chaining
     */
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

    /**
     * Sets a callback function to be executed before the query runs.
     * This allows for driver-specific customizations and advanced query modifications.
     * 
     * <p>The callback receives a Predicate that can be used to test for driver-specific
     * types and capabilities, enabling conditional logic based on the underlying provider.
     * 
     * @param beforeQuery the callback function to execute before running the query
     * @return this Query instance for method chaining
     */
    public Query beforeQuery(Consumer<Predicate<Object>> beforeQuery) {
        this.beforeQuery = beforeQuery;
        return this;
    }

    /**
     * Executes the query and returns an iterator over the matching documents.
     * 
     * <p>This method applies all the configured filters, sorting, pagination, and field selection
     * to retrieve documents from the document store. The results are returned as a DocumentIterator
     * that allows for efficient iteration over potentially large result sets.
     * 
     * <p>If specific field paths are provided, only those fields will be retrieved from each document.
     * Field paths support dot notation for accessing nested fields (e.g., "user.profile.name").
     * 
     * @param fieldPath optional field paths to retrieve. If not provided, all fields are retrieved
     * @return a DocumentIterator for iterating over the query results
     */
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

    /**
     * Initializes the query for execution by validating field paths and query constraints.
     * 
     * <p>This method performs several validation checks:
     * <ul>
     *   <li>Ensures the document store is not closed</li>
     *   <li>Validates all field paths are properly formatted</li>
     *   <li>Ensures orderBy field appears in at least one where clause (if both are specified)</li>
     * </ul>
     * 
     * @param fieldPaths the list of field paths to retrieve
     */
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
