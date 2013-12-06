package com.taobao.tddl.optimizer.costbased.esitimater.stat;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.taobao.tddl.common.utils.TddlToStringStyle;

/**
 * @author danchen
 */
public class TableStat {

    // 表名
    private String tableName;
    // 表的全部行数
    private long   tableRows;

    public long getTableRows() {
        return tableRows;
    }

    public void setTableRows(long tableRows) {
        this.tableRows = tableRows;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, TddlToStringStyle.DEFAULT_STYLE);
    }

}
