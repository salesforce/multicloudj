package com.salesforce.multicloudj.docstore.aws;

import com.salesforce.multicloudj.docstore.client.Query;
import com.salesforce.multicloudj.docstore.driver.CollectionOptions;
import com.salesforce.multicloudj.docstore.driver.Document;
import com.salesforce.multicloudj.docstore.driver.DocumentIterator;
import com.salesforce.multicloudj.docstore.driver.PaginationToken;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;

public class AwsDocumentIterator implements DocumentIterator {

    // the query runner
    private QueryRunner queryRunner = null;

    // items from the last query
    private List<Map<String, AttributeValue>> items = new ArrayList<>();

    // index of the current item in items
    private int curr = 0;

    // number of items to skip
    private int offset = 0;

    // max number of items to return
    private int limit = 0;

    // number of items returned
    private int count = 0;

    // pagination token. If a return token is empty, it means there is no more element for the request. This value
    // may be non-null even if this iterator has reached to the limit. The token then can be used by the next query.
    private AwsPaginationToken paginationToken = null;

    // lastEvaluatedKey from the last query
    private Map<String, AttributeValue> last = new HashMap<>();

    private Function<Object, Boolean> asFunc = null;

    public AwsDocumentIterator(QueryRunner queryRunner, Query query, int count) {
        this.queryRunner = queryRunner;
        this.offset = query.getOffset();
        this.limit = query.getLimit();
        this.paginationToken = (AwsPaginationToken) query.getPaginationToken();
        this.count = count;
    }

    private Map<String, AttributeValue> getKeysFromDocument(Document document, List<String> paginationKeys) {
        Map<String, AttributeValue> documentMap = AwsCodec.encodeDoc(document).m();
        Map<String, AttributeValue> keyMap = new HashMap<>();
        for (String key : paginationKeys) {
            keyMap.put(key, documentMap.get(key));
        }

        return keyMap;
    }

    @Override
    public void next(Document document) {
        if (!hasNext()) {
            throw new NoSuchElementException("No more elements");
        }

        AwsCodec.decodeDoc(AttributeValue.builder().m(items.get(curr)).build(), document);
        if (paginationToken == null) {
            paginationToken = new AwsPaginationToken();
        }
        paginationToken.setExclusiveStartKey(getKeysFromDocument(document, queryRunner.getPaginationKeys()));

        curr++;
        count++;
    }

    @Override
    public boolean hasNext() {
        // run() should have been run before this function.
        if (limit > 0 && count >= offset + limit) {
            if (last == null || last.isEmpty()) {
                if (paginationToken != null) {
                    paginationToken.setExclusiveStartKey(null);
                }
            }
            return false;
        }

        if (paginationToken == null) {
            // Move count to offset.
            while (count < offset) {
                while (curr >= items.size()) {
                    // Make a new query request at the end of the page.
                    if (last == null || last.isEmpty()) {
                        return false;
                    }
                    items.clear();
                    last = queryRunner.run(last, items, asFunc);
                    curr = 0;
                }
                curr++;
                count++;
            }
        }

        while (curr >= items.size()) {
            // Make a new query request at the end of the page.
            if (last == null || last.isEmpty()) {
                if (paginationToken != null) {
                    paginationToken.setExclusiveStartKey(null);
                }
                return false;
            }
            items.clear();
            last = queryRunner.run(last, items, asFunc);
            curr = 0;
        }

        return true;
    }

    public void run(Map<String, AttributeValue> startAfter) {
        last = queryRunner.run(startAfter, items, asFunc);
    }

    @Override
    public void stop() {
        items = null;
        last = null;
    }

    @Override
    public PaginationToken getPaginationToken() {
        return paginationToken;
    }
}
