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

    private static final long                            serialVersionUID         = 5168519373619656091L;

    /**
     * 表名
     */
    private final String                                 tableName;

    /**
     * 主键索引描述
     */
    private final Map<String/* indexName */, IndexMeta>  primaryIndexes           = new HashMap<String, IndexMeta>(4);

    /**
     * 二级索引描述
     */
    private final Map<String/* index Name */, IndexMeta> secondaryIndexes         = new HashMap<String, IndexMeta>(8);

    private final Map<String, ColumnMeta>                primaryKeys              = new HashMap<String, ColumnMeta>();
    private final Map<String, ColumnMeta>                columns                  = new HashMap<String, ColumnMeta>();
    private final Map<String, ColumnMeta>                allColumns               = new HashMap<String, ColumnMeta>();
    private final List<ColumnMeta>                       allColumnsOrderByDefined = new LinkedList<ColumnMeta>();

    private boolean                                      temp                     = false;

    private boolean                                      sortedDuplicates         = true;

    public TableMeta(String tableName, List<ColumnMeta> allColumnsOrderByDefined, IndexMeta primaryIndex,
                     List<IndexMeta> secondaryIndexes){
        this.tableName = tableName;
        if (primaryIndex != null) {
            this.primaryIndexes.put(primaryIndex.getName(), primaryIndex);
        }

        if (secondaryIndexes != null) {
            for (IndexMeta one : secondaryIndexes) {
                this.secondaryIndexes.put(one.getName(), one);
            }
        }

        this.allColumnsOrderByDefined.addAll(allColumnsOrderByDefined);
        for (ColumnMeta c : primaryIndex.getKeyColumns()) {
            this.primaryKeys.put(c.getName(), c);
            this.allColumns.put(c.getName(), c);
        }

        for (ColumnMeta c : primaryIndex.getValueColumns()) {
            this.columns.put(c.getName(), c);
            this.allColumns.put(c.getName(), c);
        }
    }

    public IndexMeta getPrimaryIndex() {
        return primaryIndexes == null ? null : primaryIndexes.values().iterator().next();
    }

    public List<IndexMeta> getSecondaryIndexes() {
        return new ArrayList(secondaryIndexes.values());
    }

    public Map<String, IndexMeta> getSecondaryIndexesMap() {
        return secondaryIndexes;
    }

    public String getTableName() {
        return tableName;
    }

    public Collection<ColumnMeta> getPrimaryKey() {
        return primaryKeys.values();
    }

    public Collection<ColumnMeta> getColumns() {
        return columns.values();
    }

    public Map<String, ColumnMeta> getPrimaryKeyMap() {
        return this.primaryKeys;
    }

    public Collection<ColumnMeta> getAllColumns() {
        return allColumnsOrderByDefined;
    }

    public IndexMeta getIndexMeta(String indexName) {
        IndexMeta retMeta = primaryIndexes.get(indexName);
        if (retMeta != null) {
            return retMeta;
        }
        retMeta = secondaryIndexes.get(indexName);
        return retMeta;
    }

    public List<IndexMeta> getIndexs() {
        List<IndexMeta> indexs = new ArrayList<IndexMeta>();
        indexs.add(this.getPrimaryIndex());
        indexs.addAll(this.getSecondaryIndexes());
        return indexs;
    }

    public ColumnMeta getColumn(String name) {
        if (name.contains(".")) {
            return allColumns.get(name.split("\\.")[1]); // 避免转义
        }
        return allColumns.get(name);
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, TddlToStringStyle.DEFAULT_STYLE);
    }

    public void setTmp(boolean b) {
        this.temp = b;
    }

    public boolean isTmp() {
        return this.temp;
    }

    public void setSortedDuplicates(boolean sortedDuplicates) {
        this.sortedDuplicates = sortedDuplicates;

    }

    public boolean issortedDuplicates() {
        return sortedDuplicates;
    }
}
