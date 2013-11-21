package com.taobao.tddl.optimizer.costbased.esitimater.stat;

import java.util.LinkedList;
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
    private String             tablename;
    // 列的统计数据
    private List<KVColumnStat> listColumnMeta;

    public TableColumnStat(String tablename){
        super();
        this.tablename = tablename;
        this.listColumnMeta = new LinkedList<KVColumnStat>();
    }

    public List<KVColumnStat> getListcoColumnMeta() {
        return listColumnMeta;
    }

    public void setListcoColumnMeta(List<KVColumnStat> listcoColumnMeta) {
        this.listColumnMeta = listcoColumnMeta;
    }

    public String getTablename() {
        return tablename;
    }

    public void addColumnMeta(KVColumnStat columnMeta) {
        if (columnMeta == null) {
            return;
        }
        listColumnMeta.add(columnMeta);
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, TddlToStringStyle.DEFAULT_STYLE);
    }

}
