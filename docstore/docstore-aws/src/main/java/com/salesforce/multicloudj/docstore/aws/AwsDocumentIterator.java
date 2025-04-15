package com.salesforce.multicloudj.docstore.aws;

import com.salesforce.multicloudj.docstore.driver.Document;
import com.salesforce.multicloudj.docstore.driver.DocumentIterator;
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

    // lastEvaluatedKey from the last query
    private Map<String, AttributeValue> last = new HashMap<>();

    private Function<Object, Boolean> asFunc = null;

    public AwsDocumentIterator(QueryRunner queryRunner, int offset, int limit, int count) {
        this.queryRunner = queryRunner;
        this.offset = offset;
        this.limit = limit;
        this.count = count;
    }

    @Override
    public void next(Document document) {
        if (!hasNext()) {
            throw new NoSuchElementException("No more elements");
        }

        AwsCodec.decodeDoc(AttributeValue.builder().m(items.get(curr)).build(), document);
        curr++;
        count++;
    }

    @Override
    public boolean hasNext() {
        if (limit > 0 && count >= offset + limit) {
            return false;
        }

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

        while (curr >= items.size()) {
            // Make a new query request at the end of the page.
            if (last == null || last.isEmpty()) {
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
}
