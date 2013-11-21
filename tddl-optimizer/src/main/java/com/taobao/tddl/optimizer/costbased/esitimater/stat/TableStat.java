package com.taobao.tddl.optimizer.costbased.esitimater.stat;

/**
 * @author danchen
 */
public class TableStat {

    //表名
    private String tableName;
    //表的全部行数
    private long   tableRows;

    public TableStat(String tableName){
        super();
        this.tableName = tableName;
    }

    public long getTableRows() {
        return tableRows;
    }

    public void setTableRows(long tableRows) {
        this.tableRows = tableRows;
    }

    public String getTableName() {
        return tableName;
    }

}
