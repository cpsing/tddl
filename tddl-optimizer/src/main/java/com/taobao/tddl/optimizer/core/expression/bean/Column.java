package com.taobao.tddl.optimizer.core.expression.bean;

import java.util.Map;

import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.IRowSet;
import com.taobao.tddl.optimizer.core.PlanVisitor;
import com.taobao.tddl.optimizer.core.expression.IColumn;

/**
 * 描述一个列
 * 
 * @author jianghang 2013-11-13 下午5:03:15
 * @since 5.1.0
 */
public class Column implements IColumn {

    protected String    alias;
    protected String    columName;
    protected String    tableName;
    protected DATA_TYPE dataType;
    protected boolean   distinct;

    private Object      value = null;
    private boolean     isNot = false;

    public Column(){
    }

    public Column(String columName, String alias, DATA_TYPE dataType){
        this.columName = columName;
        this.alias = alias;
        this.dataType = dataType;
    }

    public void serverMap(IRowSet kvPair) throws Exception {
        if (kvPair == null) {
            return;
        }
        // TODO
        Object val = null;
        // Object val = GeneralUtil.getValueByIColumn(kvPair, this);
        this.value = val;
    }

    public void serverReduce(IRowSet kvPair) throws Exception {
        serverMap(kvPair);
    }

    public IColumn assignment(Map<Integer, ParameterContext> parameterSettings) {
        return this;
    }

    public IColumn setDataType(com.taobao.tddl.optimizer.core.expression.ISelectable.DATA_TYPE data_type) {
        this.dataType = data_type;
        return this;
    }

    public com.taobao.tddl.optimizer.core.expression.ISelectable.DATA_TYPE getDataType() {
        return dataType;
    }

    public String getAlias() {
        return alias;
    }

    public String getTableName() {
        return tableName;
    }

    public String getColumnName() {
        return columName;
    }

    public IColumn setAlias(String alias) {
        this.alias = alias;
        return this;
    }

    public IColumn setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public IColumn setColumnName(String columnName) {
        this.columName = columnName;
        return this;
    }

    public String getFullName() {
        if (this.tableName != null) {
            return tableName + "." + this.getColumnName();
        } else {
            return this.getColumnName();
        }
    }

    public boolean isDistinct() {
        return distinct;
    }

    public boolean isNot() {
        return isNot;
    }

    public IColumn setDistinct(boolean distinct) {
        this.distinct = distinct;
        return this;
    }

    public IColumn setIsNot(boolean isNot) {
        this.isNot = isNot;
        return this;
    }

    public IColumn copy() {
        IColumn newColumn = ASTNodeFactory.getInstance().createColumn();
        newColumn.setColumnName(columName)
            .setAlias(alias)
            .setDataType(dataType)
            .setTableName(tableName)
            .setDistinct(isDistinct());
        return newColumn;
    }

    public int compareTo(Object o) {
        throw new NotSupportException();
    }

    public Object getResult() {
        return value;
    }

    public void accept(PlanVisitor visitor) {
        visitor.visit(this);
    }

    // ================== hashcode/equals/toString不要随意改动=================

    /**
     * 这个方法不要被自动修改！ 在很多地方都有用到。
     */
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (distinct ? 1231 : 1237);
        result = prime * result + ((columName == null) ? 0 : columName.hashCode());
        result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
        return result;
    }

    /**
     * 这个方法不要被自动修改！ 在很多地方都有用到。
     */
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }

        Column other = (Column) obj;
        // alias 都不为空的时候，进行比较，如果不匹配则返回false,其余时候都跳过alias匹配
        if (alias != null && other.alias != null) {
            if (!alias.equals(other.alias)) {
                return false;
            }
        }

        // if (dataType != other.dataType)
        // return false;
        // if (distinct != other.distinct)
        // return false;
        if (columName == null) {
            if (other.columName != null) return false;
        } else if (!columName.equals(other.columName)) return false;
        if (tableName == null) {
            if (other.tableName != null) return false;
        } else if (!tableName.equals(other.tableName)) return false;
        return true;
    }

    public String toString() {
        StringBuilder builder = new StringBuilder();
        if (this.isDistinct()) {
            builder.append("distinct ");
        }
        if (this.getTableName() != null) {
            builder.append(this.getTableName()).append(".");
        }

        builder.append(this.getColumnName());
        if (this.getAlias() != null) {
            builder.append(" as ").append(this.getAlias());
        }
        return builder.toString();
    }

}
