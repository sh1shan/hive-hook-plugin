package org.data.meta.hive.model.lineage;

import java.util.List;

public class TableLineage {
    private List<String> srcTable;
    private List<String> destTable;

    public TableLineage() {
    }

    public List<String> getSrcTable() {
        return this.srcTable;
    }

    public void setSrcTable(List<String> srcTable) {
        this.srcTable = srcTable;
    }

    public List<String> getDestTable() {
        return this.destTable;
    }

    public void setDestTable(List<String> destTable) {
        this.destTable = destTable;
    }
}
