package org.data.meta.hive.model.action;

public class HiveMetaAction {
    //对象
    private String objectType;
    //操作
    private String operationName;

    public HiveMetaAction() {
    }

    public HiveMetaAction(String objectType, String operationName) {
        this.objectType = objectType;
        this.operationName = operationName;
    }

    public String getObjectType() {
        return this.objectType;
    }

    public void setObjectType(String objectType) {
        this.objectType = objectType;
    }

    public String getOperationName() {
        return this.operationName;
    }

    public void setOperationName(String operationName) {
        this.operationName = operationName;
    }
}
