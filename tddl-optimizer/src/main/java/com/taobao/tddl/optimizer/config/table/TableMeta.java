package com.taobao.tddl.optimizer.config.table;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.taobao.tddl.common.utils.TddlToStringStyle;

/**
 * 一个table的描述，包含主键信息/字段信息/索引信息等，暂时不考虑外键/约束键，目前没意义
 * 
 * @author jianxing <jianxing.qx@taobao.com>
 * @author whisper
 */
public class TableMeta implements Serializable, Cloneable {

    private static final long                      serialVersionUID         = 5168519373619656091L;

    /**
     * 表名
     */
    private String                                 tableName;

    /**
     * 主键索引描述
     */
    private Map<String/* indexName */, IndexMeta>  primaryIndexes           = new HashMap<String, IndexMeta>(4);

    /**
     * 二级索引描述
     */
    private Map<String/* index Name */, IndexMeta> secondaryIndexes         = new HashMap<String, IndexMeta>(8);

    private boolean                                tmp;                                                         // 是否为临时表
    private boolean                                sortedDuplicates;

    private Map<String, ColumnMeta>                primaryKey               = new HashMap<String, ColumnMeta>();
    private Map<String, ColumnMeta>                columns                  = new HashMap<String, ColumnMeta>();
    private Map<String, ColumnMeta>                allColumns               = new HashMap<String, ColumnMeta>();
    private List<ColumnMeta>                       allColumnsOrderByDefined = new LinkedList<ColumnMeta>();

    public TableMeta(){
    }

    public TableMeta(String tableName){
        this.tableName = tableName;
    }

    public TableMeta(String tableName, List<ColumnMeta> allColumnsOrderByDefined, IndexMeta primaryIndex,
                       IndexMeta[] secondaryIndexes){
        this.tableName = tableName;
        if (primaryIndex != null) {
            this.primaryIndexes.put(primaryIndex.getName(), primaryIndex);
        }
        if (secondaryIndexes != null) {
            for (IndexMeta one : secondaryIndexes) {
                this.secondaryIndexes.put(one.getName(), one);
            }
        }

        this.allColumnsOrderByDefined = allColumnsOrderByDefined;
        for (ColumnMeta c : primaryIndex.getKeyColumns()) {
            this.primaryKey.put(c.getName(), c);
            this.allColumns.put(c.getName(), c);
        }

        for (ColumnMeta c : primaryIndex.getValueColumns()) {
            this.columns.put(c.getName(), c);
            this.allColumns.put(c.getName(), c);
        }
    }

    public Map<String, IndexMeta> getPrimaryIndexes() {
        return primaryIndexes;
    }

    public void setPrimaryIndexes(Map<String, IndexMeta> primaryIndexes) {
        this.primaryIndexes = primaryIndexes;
    }

    public void setSecondaryIndexes(Map<String, IndexMeta> secondaryIndexes) {
        this.secondaryIndexes = secondaryIndexes;
    }

    public Map<String, IndexMeta> getSecondaryIndexesMap() {
        return secondaryIndexes;
    }

    public void setPrimaryKey(Map<String, ColumnMeta> primaryKey) {
        this.primaryKey = primaryKey;
    }

    public String getPrimaryIndexName() {
        return this.primaryIndexes.keySet().iterator().next();
    }

    public void setTmp(boolean tmp) {
        this.tmp = tmp;
    }

    public boolean getTmp() {
        return tmp;
    }

    public boolean getSortedDuplicates() {
        return sortedDuplicates;
    }

    public void setSortedDuplicates(boolean sortedDuplicates) {
        this.sortedDuplicates = sortedDuplicates;
    }

    public void setColumns(ColumnMeta[] columns) {
        for (ColumnMeta c : columns) {
            this.columns.put(c.getName(), c);
            this.allColumns.put(c.getName(), c);
        }
    }

    public void setPrimaryIndex(IndexMeta primaryIndex) {
        if (primaryIndex == null) {
            return;
        }

        if (!primaryIndexes.containsKey(primaryIndex.getName())) {
            primaryIndexes.put(primaryIndex.getName(), primaryIndex);
        }
    }

    public void setPrimaryKey(ColumnMeta[] primaryKey) {
        for (ColumnMeta c : primaryKey) {
            this.primaryKey.put(c.getName(), c);
            this.allColumns.put(c.getName(), c);
        }
    }

    public void addSecondaryIndex(IndexMeta secondaryIndex) {
        if (secondaryIndex == null) {
            return;
        }

        this.secondaryIndexes.put(secondaryIndex.getName(), secondaryIndex);
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Collection<ColumnMeta> getPrimaryKey() {
        return primaryKey.values();
    }

    public Collection<ColumnMeta> getColumns() {
        return columns.values();
    }

    public Map<String, ColumnMeta> getPrimaryKeyMap() {
        return this.primaryKey;
    }

    public Collection<ColumnMeta> getAllColumns() {
        return allColumnsOrderByDefined;
    }

    public IndexMeta getPrimaryIndex() {
        return primaryIndexes == null ? null : primaryIndexes.values().iterator().next();
    }

    public List<IndexMeta> getSecondaryIndexes() {
        return new ArrayList(secondaryIndexes.values());
    }

    public IndexMeta getIndexMeta(String indexName) {
        IndexMeta retMeta = primaryIndexes.get(indexName);
        if (retMeta != null) {
            return retMeta;
        }
        retMeta = secondaryIndexes.get(indexName);
        return retMeta;
    }

    public List<IndexMeta> getPrimaryKeyIndexs() {
        List<IndexMeta> res = new LinkedList<IndexMeta>();
        res.add(this.getPrimaryIndex());
        return res;
    }

    public List<IndexMeta> getIndexs() {
        List<IndexMeta> indexs = new ArrayList<IndexMeta>();
        indexs.addAll(this.getPrimaryKeyIndexs());
        indexs.addAll(this.getSecondaryIndexes());
        return indexs;
    }

    public ColumnMeta getColumn(String name) {
        if (name.contains(".")) {
            return allColumns.get(name.split("\\.")[1]); // 避免转义
        }
        return allColumns.get(name);
    }

    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, TddlToStringStyle.DEFAULT_STYLE);
    }
}
