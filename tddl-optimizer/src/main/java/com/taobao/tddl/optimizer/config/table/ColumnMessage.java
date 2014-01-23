package com.taobao.tddl.optimizer.config.table;

import java.io.Serializable;

import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * 列信息
 * 
 * @author whisper
 */
public class ColumnMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 列名
     */
    protected final String    name;
    /**
     * 当前列的类型
     */
    protected final DataType  dataType;
    /**
     * 当前列的别名
     */
    protected final String    alias;
    /**
     * 是否准许为空
     */
    protected final boolean   nullable;

    protected final boolean   isAutoCreated;

    public ColumnMessage(String name, DataType dataType, String alias){
        this(name, dataType, alias, true, false);
    }

    public ColumnMessage(String name, DataType dataType, String alias, boolean isAutoCreated){
        this(name, dataType, alias, true, isAutoCreated);
    }

    public ColumnMessage(String name, DataType dataType, String alias, boolean nullable, boolean isAutoCreated){
        this.name = name;
        this.dataType = dataType;
        this.alias = alias;
        this.nullable = nullable;
        this.isAutoCreated = isAutoCreated;
    }

    public ColumnMessage(String name, DataType dataType){
        this(name, dataType, null);
    }

    public boolean getNullable() {
        return nullable;
    }

    public String getAlias() {
        return alias;
    }

    public DataType getDataType() {
        return dataType;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "ColumnMeta [" + (name != null ? "name=" + name + ", " : "") + (alias != null ? "alias=" + alias : "")
               + "]";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final ColumnMessage other = (ColumnMessage) obj;
        if ((this.name == null) ? (other.name != null) : !this.name.equals(other.name)) {
            return false;
        }
        if (this.dataType != other.dataType) {
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

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 23 * hash + (this.name != null ? this.name.hashCode() : 0);
        hash = 23 * hash + (this.dataType != null ? this.dataType.hashCode() : 0);
        hash = 23 * hash + (this.alias != null ? this.alias.hashCode() : 0);
        hash = 23 * hash + (this.nullable ? 1 : 0);
        return hash;
    }

    public String toStringWithInden(String parentTableName) {
        StringBuilder sb = new StringBuilder();
        sb.append(name);
        if (alias != null) {
            sb.append(" as ").append(alias);
        }

        return sb.toString();
    }

    public boolean isAutoCreated() {
        return isAutoCreated;
    }

}
