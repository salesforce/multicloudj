package com.salesforce.multicloudj.docstore.ali;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.sql.SQLQueryRequest;
import com.alicloud.openservices.tablestore.model.sql.SQLQueryResponse;
import com.alicloud.openservices.tablestore.model.sql.SQLResultSet;
import com.alicloud.openservices.tablestore.model.sql.SQLRow;
import com.alicloud.openservices.tablestore.model.sql.SQLTableMeta;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.salesforce.multicloudj.docstore.ali.AliDocumentIterator.INIT_TOKEN;

/**
 * QueryRunner stores the information about how to query/scan the data from the tablestore.
 * and exposes a run method to execute the query and get the data.
 */
@AllArgsConstructor
@Getter
public class QueryRunner {

    private SyncClient tableStoreClient;

    private SQLQueryRequest sqlQueryRequest;

    private boolean isScan;

    private Consumer<Predicate<Object>> beforeRun;

    public String queryPlan() {
        if (isScan) {
            return "Scan";
        }

        return "Table";
    }

    /**
     * this method is used to execute the query/scan and get the data
     *
     * @param searchToken: if the previous query/scan response contains searchToken, include it here for next page
     * @param itemsScan:   data from the scan output
     * @param items:       data from the query output
     * @param asFunc:      placeholder for beforeRun function
     * @return the search token for the next page if any. An empty or null search token means no more data.
     */
    public String run(String searchToken, List<Row> itemsScan, List<Map<String, Object>> items, Function<Object, Boolean> asFunc) {
        if (!StringUtils.isEmpty(searchToken) && !searchToken.equals(INIT_TOKEN)) {
            sqlQueryRequest.setSearchToken(searchToken);
        }
        SQLQueryResponse response = tableStoreClient.sqlQuery(sqlQueryRequest);

        SQLResultSet resultSet = response.getSQLResultSet();
        SQLTableMeta meta = resultSet.getSQLTableMeta();

        while (resultSet.hasNext()) {
            SQLRow row = resultSet.next();

            Map<String, Object> keyMap = new HashMap<>();
            for (Map.Entry<String, Integer> col : meta.getColumnsMap().entrySet()) {
                String columnName = col.getKey();
                Object value = row.get(col.getValue());
                keyMap.put(columnName, value);
            }
            items.add(keyMap);
        }
        return response.getNextSearchToken();
    }
}
