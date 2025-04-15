package com.salesforce.multicloudj.docstore.ali;

import com.alicloud.openservices.tablestore.model.Row;
import com.salesforce.multicloudj.docstore.driver.Document;
import com.salesforce.multicloudj.docstore.driver.DocumentIterator;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;

public class AliDocumentIterator implements DocumentIterator {
    // the query runner
    private final QueryRunner queryRunner;

    int curr = 0;

    int offset = 0 ;
    int limit  = 0;
    int count = 0;

    List<Row> scanItems = new ArrayList<>();

    List<Map<String, Object>> queryItems = new ArrayList<>();
    public static final String INIT_TOKEN = "INIT_TOKEN";
    // lastEvaluatedKey from the last query
    private String lastToken;

    private Function<Object, Boolean> asFunc = null;

    public AliDocumentIterator(QueryRunner qr, int offset, int limit) {
        this.queryRunner = qr;
        this.offset = offset;
        this.limit = limit;
    }

    @Override
    public void next(Document document) {
        if (!hasNext()) {
            throw new NoSuchElementException("No more elements");
        }
        if (queryRunner.getSqlQueryRequest() != null) {
            AliCodec.decodeDoc(this.queryItems.get(curr++), document);
        } else {
            AliCodec.decodeDoc(this.scanItems.get(curr++), document);
        }
        count++;

    }

    @Override
    public boolean hasNext() {
        if (limit > 0 && count >= limit) {
            return false;
        }

        if (queryRunner.getSqlQueryRequest() != null) {
            while (curr >= queryItems.size()) {
                // Make a new query request at the end of the page.
                if (StringUtils.isEmpty(lastToken)) {
                    return false;
                }
                queryItems.clear();
                lastToken = queryRunner.run(lastToken, scanItems, queryItems, asFunc);
                curr = 0;
            }
        } else {
            while (curr >= scanItems.size()) {
                // Make a new query request at the end of the page.
                if (StringUtils.isEmpty(lastToken)) {
                    return false;
                }
                scanItems.clear();
                lastToken = queryRunner.run(lastToken, scanItems, queryItems, asFunc);
                curr = 0;
            }
        }


        return true;
    }

    public void run(String lastToken) {
        this.lastToken = queryRunner.run(lastToken, scanItems, queryItems, asFunc);
    }

    @Override
    public void stop() {
        lastToken = null;
        scanItems = null;
        queryItems = null;
    }
}
