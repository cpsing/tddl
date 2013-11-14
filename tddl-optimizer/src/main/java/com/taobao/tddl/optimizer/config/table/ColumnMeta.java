package com.taobao.tddl.optimizer.config.table;

import java.io.Serializable;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.taobao.tddl.common.utils.TddlToStringStyle;
import com.taobao.tddl.optimizer.core.expression.ISelectable.DATA_TYPE;

/**
 * Column 的元信息描述
 * 
 * @author whisper
 */
public class ColumnMeta implements Serializable {

    private static final long serialVersionUID = 1748510851861759314L;

    /**
     * 表名
     */
    private final String      tableName;

    /**
     * 列名
     */
    protected final String    name;

    /**
     * 当前列的类型
     */
    protected final DATA_TYPE dataType;

    /**
     * 当前列的别名
     */
    protected final String    alias;

    /**
     * 是否准许为空
     */
    protected final boolean   nullable;

    public ColumnMeta(String tableName, String name, DATA_TYPE dataType, String alias){
        this(tableName, name, dataType, alias, true);
    }

    public ColumnMeta(String tableName, String name, DATA_TYPE dataType){
        this(tableName, name, dataType, null, true);
    }

    public ColumnMeta(String tableName, String name, DATA_TYPE dataType, String alias, boolean nullable){
        this.tableName = tableName;
        this.name = name;
        this.dataType = dataType;
        this.alias = alias;
        this.nullable = nullable;
    }

    public String getTableName() {
        return tableName;
    }

    public String getName() {
        return name;
    }

    public DATA_TYPE getDataType() {
        return dataType;
    }

    public String getAlias() {
        return alias;
    }

    public boolean isNullable() {
        return nullable;
    }

    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final ColumnMeta other = (ColumnMeta) obj;
        if ((this.name == null) ? (other.name != null) : !this.name.equals(other.name)) {
            return false;
        }
        if (this.dataType != other.dataType) {
            return false;
        }
        if ((this.tableName == null) ? (other.tableName != null) : !this.tableName.equals(other.tableName)) {
            return false;
        }
        if ((this.alias == null) ? (other.alias != null) : !this.alias.equals(other.alias)) {
            return false;
        }
        if (this.nullable != other.nullable) {
            return false;
        }
        return true;
    }

    public int hashCode() {
        int hash = 5;
        hash = 23 * hash + (this.name != null ? this.name.hashCode() : 0);
        hash = 23 * hash + (this.dataType != null ? this.dataType.hashCode() : 0);
        hash = 23 * hash + (this.tableName != null ? this.tableName.hashCode() : 0);
        hash = 23 * hash + (this.alias != null ? this.alias.hashCode() : 0);
        hash = 23 * hash + (this.nullable ? 1 : 0);
        return hash;
    }

    public String toStringWithInden(String parentTableName) {
        StringBuilder sb = new StringBuilder();

        sb.append(tableName).append(".");

        sb.append(name);
        if (alias != null) {
            sb.append(" as ").append(alias);
        }

        return sb.toString();
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, TddlToStringStyle.DEFAULT_STYLE);
    }

}
