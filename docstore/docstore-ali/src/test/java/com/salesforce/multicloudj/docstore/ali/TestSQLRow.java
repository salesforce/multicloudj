package com.salesforce.multicloudj.docstore.ali;

import com.alicloud.openservices.tablestore.model.sql.SQLRow;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.Map;

class TestSQLRow implements SQLRow {
    Map<Integer, String> vals1 = Map.of(0, "test1", 1, "test2");
    Map<String, Integer> vals2 = Map.of( "test1", 0,  "test2", 1);
    public TestSQLRow() {

    }

    @Override
    public Object get(int var1) {
        return vals1.get(var1);
    }

    @Override
    public Object get(String var1) {
        return vals2.get(var1);
    }

    @Override
    public String getString(int i) {
        return "";
    }

    @Override
    public String getString(String s) {
        return "";
    }

    @Override
    public Long getLong(int i) {
        return 0L;
    }

    @Override
    public Long getLong(String s) {
        return 0L;
    }

    @Override
    public Boolean getBoolean(int i) {
        return null;
    }

    @Override
    public Boolean getBoolean(String s) {
        return null;
    }

    @Override
    public Double getDouble(int i) {
        return 0.0;
    }

    @Override
    public Double getDouble(String s) {
        return 0.0;
    }

    @Override
    public ZonedDateTime getDateTime(int i) {
        return null;
    }

    @Override
    public ZonedDateTime getDateTime(String s) {
        return null;
    }

    @Override
    public Duration getTime(int i) {
        return null;
    }

    @Override
    public Duration getTime(String s) {
        return null;
    }

    @Override
    public LocalDate getDate(int i) {
        return null;
    }

    @Override
    public LocalDate getDate(String s) {
        return null;
    }

    @Override
    public ByteBuffer getBinary(int i) {
        return null;
    }

    @Override
    public ByteBuffer getBinary(String s) {
        return null;
    }

    @Override
    public String toDebugString() {
        return "";
    }

}