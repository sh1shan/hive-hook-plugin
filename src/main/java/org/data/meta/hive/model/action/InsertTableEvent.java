package org.data.meta.hive.model.action;


import org.apache.hadoop.hive.ql.plan.HiveOperation;

import java.util.List;
import java.util.Map;

public class InsertTableEvent extends HiveMetaAction {
    private String db;
    private String table;
    private Map<String, String> keyValues;
    private List<String> files;

    public InsertTableEvent() {
        super("Table", HiveOperation.QUERY.getOperationName());
    }

    public String getDb() {
        return db;
    }

    public String getTable() {
        return table;
    }

    public Map<String, String> getKeyValues() {
        return keyValues;
    }

    public List<String> getFiles() {
        return files;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public void setKeyValues(Map<String, String> keyValues) {
        this.keyValues = keyValues;
    }

    public void setFiles(List<String> files) {
        this.files = files;
    }


}
