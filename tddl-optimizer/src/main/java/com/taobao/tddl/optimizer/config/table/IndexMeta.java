package com.taobao.tddl.optimizer.config.table;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.common.utils.TddlToStringStyle;
import com.taobao.tddl.optimizer.utils.OptimizerUtils;

/**
 * 每一个索引（或者叫KV更直白点）的描述
 * 
 * @author jianxing <jianxing.qx@taobao.com>
 */
public class IndexMeta implements Serializable, Cloneable {

    private static final long       serialVersionUID     = 1L;
    /**
     * 表名+列名
     */
    private String                  name                 = "";
    /**
     * 列名字
     */
    private ColumnMeta[]            keyColumns;
    /**
     * 当前index的类型 @IndexType
     */
    private int                     indexType;
    /**
     * 值名字
     */
    private ColumnMeta[]            valueColumns;

    /**
     * 保存了所有列，方便查找
     */
    private Map<String, ColumnMeta> columnsMap           = new HashMap();
    /**
     * 关系，用来处理多对多关系，暂时没有用到。 see IndexType
     */
    private int                     relationship;
    /**
     * 表名+index名的方式进行命名的。 在查询时，会先根据.之前的，拿到表名，然后找到对应的schema。
     * 然后再根据.之后的，找到对应的schema
     */
    private String                  tableName;
    /**
     * 是否强同步,目前只支持主key
     */
    private boolean                 isStronglyConsistent = true;
    private boolean                 isPK                 = false;
    /**
     * 该索引的拆分键
     */
    List<ColumnMeta>                partitionColumns     = new ArrayList();
    private List<String>            dbNames              = new ArrayList();
    private TableMeta               tableMeta            = null;
    private String                  nameWithOutDot;

    public IndexMeta(String tableName, ColumnMeta[] columns, int indexType, ColumnMeta[] values, int relationship,
                     boolean isPK, List<ColumnMeta> partitionColumns, TableMeta tableMeta){
        this.tableName = tableName;
        this.keyColumns = OptimizerUtils.uniq(columns);
        this.indexType = indexType;
        this.valueColumns = OptimizerUtils.uniq(values);
        this.relationship = relationship;
        setIndexNameByTableAndColumns(tableName, columns);
        this.isPK = isPK;
        this.partitionColumns = partitionColumns;
        this.tableMeta = tableMeta;
        initColumnsMap();

    }

    void initColumnsMap() {
        if (valueColumns != null) for (ColumnMeta cm : valueColumns)
            this.columnsMap.put(cm.getName(), cm);

        if (keyColumns != null) for (ColumnMeta cm : keyColumns)
            this.columnsMap.put(cm.getName(), cm);
    }

    public IndexMeta(String tableName, ColumnMeta[] columns, int indexType, ColumnMeta[] values, int relationship,
                     boolean isPK, boolean isStronglyConsistent, List<ColumnMeta> partitionColumns, TableMeta tableMeta){
        this.tableName = tableName;
        this.keyColumns = OptimizerUtils.uniq(columns);
        this.indexType = indexType;
        this.valueColumns = OptimizerUtils.uniq(values);
        this.relationship = relationship;
        setIndexNameByTableAndColumns(tableName, columns);
        this.isPK = isPK;
        this.isStronglyConsistent = isStronglyConsistent;
        this.partitionColumns = partitionColumns;
        this.tableMeta = tableMeta;
        initColumnsMap();
    }

    public TableMeta getTableMeta() {
        return tableMeta;
    }

    public void setSchema(TableMeta tableMeta) {
        this.tableMeta = tableMeta;
    }

    public void addDbName(String t) {
        this.dbNames.add(t);
    }

    public Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException e) {
            throw new IllegalArgumentException("should not be here");
        }
    }

    public ColumnMeta getColumnMeta(String name) {
        return this.columnsMap.get(name);
    }

    public List<String> getDbNames() {
        return this.dbNames;
    }

    public int getIndexType() {
        return indexType;
    }

    public ColumnMeta[] getKeyColumns() {
        return keyColumns;
    }

    public String getName() {
        return name;
    }

    public String getNameWithOutDot() {
        return nameWithOutDot;
    }

    public List<ColumnMeta> getPartitionColumns() {
        return partitionColumns;
    }

    public int getRelationship() {
        return relationship;
    }

    public String getTableName() {
        return tableName;
    }

    public ColumnMeta[] getValueColumns() {
        return valueColumns;
    }

    public boolean isPrimaryKeyIndex() {
        return this.isPK;
    }

    public boolean isStronglyConsistent() {
        return isStronglyConsistent;
    }

    private void setIndexNameByTableAndColumns(String tableName, ColumnMeta[] columns) {
        StringBuilder sb = new StringBuilder();
        sb.append(tableName).append(".");
        for (ColumnMeta column : columns) {
            sb.append("_" + column.getName());
        }
        this.name = sb.toString();
        this.nameWithOutDot = name.replace(".", "");
    }

    public void setPartitionColumns(List<ColumnMeta> partitionColumns) {
        this.partitionColumns = partitionColumns;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, TddlToStringStyle.DEFAULT_STYLE);
    }

    public String toStringWithInden(int inden) {
        StringBuilder sb = new StringBuilder();
        String tabTittle = GeneralUtil.getTab(inden);
        sb.append(tabTittle).append("[");
        sb.append("indexMeta name : ").append(name).append("\n");
        ColumnMeta[] metas = keyColumns;
        buildMetas(inden, sb, metas, "keyColumn :");
        metas = valueColumns;
        buildMetas(inden, sb, metas, "valueColumn :");
        sb.append(tabTittle).append("]");
        return sb.toString();
    }

    private void buildMetas(int inden, StringBuilder sb, ColumnMeta[] metas, String keyName) {
        if (metas != null) {
            String tabContent = GeneralUtil.getTab(inden + 1);
            sb.append(tabContent).append(keyName).append("\n");
            String content = GeneralUtil.getTab(inden + 2);
            sb.append(content);
            for (ColumnMeta meta : metas) {
                sb.append(meta.toStringWithInden(tableName));
                sb.append(" ");
            }
            sb.append("\n");
        }
    }

}
