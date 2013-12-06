package com.taobao.tddl.optimizer.costbased.esitimater.stat;

import java.util.List;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.taobao.tddl.common.utils.TddlToStringStyle;

/**
 * 一个表的列的统计数据
 * 
 * @author danchen
 */
public class TableColumnStat {

    // 表名
    private String             tableName;
    // 列的统计数据
    private List<KVColumnStat> columnStats;

    public List<KVColumnStat> getColumnStats() {
        return columnStats;
    }

    public void setColumnStats(List<KVColumnStat> columnStats) {
        this.columnStats = columnStats;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void addColumnStat(KVColumnStat columnStat) {
        if (columnStat == null) {
            return;
        }
        columnStats.add(columnStat);
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, TddlToStringStyle.DEFAULT_STYLE);
    }

}
