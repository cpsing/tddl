package com.taobao.tddl.optimizer.config.table;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.utils.GeneralUtil;

/**
 * 每一个索引（或者叫KV更直白点）的描述
 * 
 * @author jianxing <jianxing.qx@taobao.com>
 */
public class IndexMeta implements Serializable, Cloneable {

    private static final long       serialVersionUID = 1L;
    /**
     * 表名+列名
     */
    private final String            name;
    /**
     * 列名字
     */
    private final List<ColumnMeta>  keyColumns;

    /**
     * 值名字
     */
    private final List<ColumnMeta>  valueColumns;

    /**
     * 当前index的类型 @IndexType
     */
    private final IndexType         indexType;

    /**
     * 关系，用来处理多对多关系，暂时没有用到。 see IndexType
     */
    private final Relationship      relationship;

    /**
     * 是否强同步,目前只支持主key
     */
    private final boolean           isStronglyConsistent;
    private final boolean           isPrimaryKeyIndex;

    /**
     * 该索引的拆分键
     */
    private final List<ColumnMeta>  partitionColumns;

    // ================== 冗余字段 ==============

    /**
     * 表名+index名的方式进行命名的。 在查询时，会先根据.之前的，拿到表名，然后找到对应的schema。
     * 然后再根据.之后的，找到对应的schema
     */
    private final String            tableName;
    /**
     * 保存了所有列，方便查找
     */
    private Map<String, ColumnMeta> columnsMap       = new HashMap();

    public IndexMeta(String tableName, List<ColumnMeta> keys, List<ColumnMeta> values, IndexType indexType,
                     Relationship relationship, boolean isStronglyConsistent, boolean isPrimaryKeyIndex,
                     List<ColumnMeta> partitionColumns){
        this.tableName = tableName;
        this.keyColumns = uniq(keys);
        this.valueColumns = uniq(values);
        this.indexType = indexType;
        this.relationship = relationship;
        this.isPrimaryKeyIndex = isPrimaryKeyIndex;
        this.isStronglyConsistent = isStronglyConsistent;
        this.partitionColumns = partitionColumns;
        this.name = buildName(tableName, keys);
        this.columnsMap = buildColumnsMap();

    }

    private Map<String, ColumnMeta> buildColumnsMap() {
        Map<String, ColumnMeta> columnsMap = new HashMap();
        if (valueColumns != null) {
            for (ColumnMeta cm : valueColumns) {
                columnsMap.put(cm.getName(), cm);
            }
        }

        if (keyColumns != null) {
            for (ColumnMeta cm : keyColumns) {
                columnsMap.put(cm.getName(), cm);
            }
        }

        return columnsMap;
    }

    private String buildName(String tableName, List<ColumnMeta> columns) {
        StringBuilder sb = new StringBuilder();
        sb.append(tableName).append(".");
        for (ColumnMeta column : columns) {
            sb.append("_" + column.getName());
        }
        return sb.toString();
    }

    public ColumnMeta getColumnMeta(String name) {
        return this.columnsMap.get(name);
    }

    public String getName() {
        if (isPrimaryKeyIndex) {
            return tableName; // 如果是主键索引，直接返回逻辑表名
        } else {
            return name; // 否则返回索引名，逻辑表名+字段
        }
    }

    public List<ColumnMeta> getKeyColumns() {
        return keyColumns;
    }

    public List<ColumnMeta> getValueColumns() {
        return valueColumns;
    }

    public IndexType getIndexType() {
        return indexType;
    }

    public Relationship getRelationship() {
        return relationship;
    }

    public boolean isStronglyConsistent() {
        return isStronglyConsistent;
    }

    public boolean isPrimaryKeyIndex() {
        return isPrimaryKeyIndex;
    }

    public List<ColumnMeta> getPartitionColumns() {
        return partitionColumns;
    }

    public String getTableName() {
        return tableName;
    }

    public String getNameWithOutDot() {
        return name.replace(".", "$");
    }

    public String toString() {
        return toStringWithInden(0);
    }

    /**
     * 根据列名获取对应的index key column
     * 
     * @param name
     * @return
     */
    public ColumnMeta getKeyColumn(String name) {
        for (ColumnMeta column : keyColumns) {
            if (column.getName().equals(name)) {
                return column;
            }
        }

        return null;
    }

    /**
     * 根据列名获取对应的index value column
     * 
     * @param name
     * @return
     */
    public ColumnMeta getValueColumn(String name) {
        for (ColumnMeta column : valueColumns) {
            if (column.getName().equals(name)) {
                return column;
            }
        }

        return null;
    }

    private List<ColumnMeta> uniq(List<ColumnMeta> s) {
        if (s == null) {
            return null;
        }

        List<ColumnMeta> uniqList = new ArrayList<ColumnMeta>(s.size());
        for (ColumnMeta cm : s) {
            if (!uniqList.contains(cm)) {
                uniqList.add(cm);
            }
        }

        return uniqList;
    }

    public String toStringWithInden(int inden) {
        StringBuilder sb = new StringBuilder();
        String tabTittle = GeneralUtil.getTab(inden);
        sb.append(tabTittle).append("[");
        sb.append("indexMeta name : ").append(name).append("\n");
        buildMetas(inden, sb, keyColumns, "keyColumn :");
        buildMetas(inden, sb, valueColumns, "valueColumn :");
        sb.append(tabTittle).append("]");
        return sb.toString();
    }

    private void buildMetas(int inden, StringBuilder sb, List<ColumnMeta> metas, String keyName) {
        if (metas != null) {
            String tabContent = GeneralUtil.getTab(inden + 1);
            sb.append(tabContent).append(keyName).append("\n");
            String content = GeneralUtil.getTab(inden + 2);
            sb.append(content);
            for (ColumnMeta meta : metas) {
                sb.append(meta.toStringWithInden(0));
                sb.append(" ");
            }

            sb.append("\n");
        }
    }

}
