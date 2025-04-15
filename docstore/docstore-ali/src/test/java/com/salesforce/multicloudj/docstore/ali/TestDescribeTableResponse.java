package com.salesforce.multicloudj.docstore.ali;

import com.alicloud.openservices.tablestore.model.ColumnType;
import com.alicloud.openservices.tablestore.model.sql.SQLColumnSchema;
import com.alicloud.openservices.tablestore.model.sql.SQLResultSet;
import com.alicloud.openservices.tablestore.model.sql.SQLRow;
import com.alicloud.openservices.tablestore.model.sql.SQLTableMeta;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class TestDescribeTableResponse implements SQLResultSet {
    private final SQLTableMeta tableMeta;
    private final List<SQLRow> rows;
    private int currentIndex;

    public TestDescribeTableResponse() {
        List<SQLColumnSchema> schema = new ArrayList<>();
        schema.add(new SQLColumnSchema("test1", ColumnType.STRING));
        schema.add(new SQLColumnSchema("test2", ColumnType.STRING));

        this.tableMeta = new SQLTableMeta(schema, Map.of("test1", 0, "test2", 1));

        // Create dummy rows
        this.rows = new ArrayList<>();
        rows.add(0, new TestSQLRow());
        rows.add(1, new TestSQLRow());

        // Set the current index to -1 (before the first element)
        this.currentIndex = -1;
    }

    @Override
    public SQLTableMeta getSQLTableMeta() {
        return tableMeta;
    }

    @Override
    public boolean hasNext() {
        return currentIndex + 1 < rows.size();
    }

    @Override
    public SQLRow next() {
        if (hasNext()) {
            currentIndex++;
            return rows.get(currentIndex);
        }
        throw new IllegalStateException("No more elements");
    }

    @Override
    public long rowCount() {
        return rows.size();
    }

    @Override
    public boolean absolute(int var1) {
        if (var1 >= 0 && var1 < rows.size()) {
            currentIndex = var1 - 1; // Set index to just before the requested position
            return true;
        }
        return false;
    }
}