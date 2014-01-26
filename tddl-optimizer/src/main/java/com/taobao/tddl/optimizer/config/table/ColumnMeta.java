package com.taobao.tddl.optimizer.config.table;

import java.io.Serializable;

import org.apache.commons.lang.StringUtils;

import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.optimizer.core.datatype.DataType;

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
    protected final DataType  dataType;

    /**
     * 当前列的别名
     */
    protected final String    alias;

    /**
     * 是否准许为空
     */
    protected final boolean   nullable;

    private String            fullName;

    public ColumnMeta(String tableName, String name, DataType dataType, String alias, boolean nullable){
        this.tableName = StringUtils.upperCase(tableName);
        this.name = StringUtils.upperCase(name);
        this.alias = StringUtils.upperCase(alias);
        this.dataType = dataType;
        this.nullable = nullable;
    }

    public String getTableName() {
        return tableName;
    }

    public String getName() {
        return name;
    }

    public DataType getDataType() {
        return dataType;
    }

    public String getAlias() {
        return alias;
    }

    public boolean isNullable() {
        return nullable;
    }

    @Override
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

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 23 * hash + (this.name != null ? this.name.hashCode() : 0);
        hash = 23 * hash + (this.dataType != null ? this.dataType.hashCode() : 0);
        hash = 23 * hash + (this.tableName != null ? this.tableName.hashCode() : 0);
        hash = 23 * hash + (this.alias != null ? this.alias.hashCode() : 0);
        hash = 23 * hash + (this.nullable ? 1 : 0);
        return hash;
    }

    public String toStringWithInden(int inden) {
        StringBuilder sb = new StringBuilder();
        String tabTittle = GeneralUtil.getTab(inden);
        sb.append(tabTittle).append(tableName).append(".");
        sb.append(name);
        if (alias != null) {
            sb.append(" as ").append(alias);
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toStringWithInden(0);
    }

    public String getFullName() {
        if (this.fullName == null) {
            String cn = this.getAlias() != null ? this.getAlias() : this.getName();
            String tableName = this.getTableName() == null ? "" : this.getTableName();
            StringBuilder sb = new StringBuilder(tableName.length() + 1 + cn.length());
            sb.append(this.getTableName());
            sb.append('.');
            sb.append(cn);

            this.fullName = sb.toString();
        }
        return this.fullName;
    }

}
