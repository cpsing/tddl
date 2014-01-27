package com.taobao.tddl.optimizer.core.expression.bean;

import java.util.Map;

import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.PlanVisitor;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.ISelectable;

/**
 * 描述一个列
 * 
 * @author jianghang 2013-11-13 下午5:03:15
 * @since 5.0.0
 */
public class Column implements IColumn {

    protected String   alias;
    protected String   columName;
    protected String   tableName;
    protected DataType dataType;
    protected boolean  distinct;
    private boolean    isNot = false;

    public Column(){
    }

    public Column(String columName, String alias, DataType dataType){
        this.columName = columName;
        this.alias = alias;
        this.dataType = dataType;
    }

    @Override
    public IColumn assignment(Map<Integer, ParameterContext> parameterSettings) {
        return this;
    }

    @Override
    public IColumn setDataType(DataType data_type) {
        this.dataType = data_type;
        return this;
    }

    @Override
    public DataType getDataType() {
        return dataType;
    }

    @Override
    public String getAlias() {
        return alias;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public String getColumnName() {
        return columName;
    }

    @Override
    public IColumn setAlias(String alias) {
        this.alias = alias;
        return this;
    }

    @Override
    public IColumn setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    @Override
    public IColumn setColumnName(String columnName) {
        this.columName = columnName;
        return this;
    }

    @Override
    public boolean isSameName(ISelectable select) {
        String cn1 = this.getColumnName();
        String cn2 = select.getColumnName();
        if (TStringUtil.isNotEmpty(select.getAlias())) {
            cn2 = select.getAlias();
        }

        return TStringUtil.equals(cn1, cn2);
    }

    @Override
    public String getFullName() {
        if (this.tableName != null) {
            return tableName + "." + this.getColumnName();
        } else {
            return this.getColumnName();
        }
    }

    @Override
    public boolean isDistinct() {
        return distinct;
    }

    @Override
    public boolean isNot() {
        return isNot;
    }

    @Override
    public IColumn setDistinct(boolean distinct) {
        this.distinct = distinct;
        return this;
    }

    @Override
    public IColumn setIsNot(boolean isNot) {
        this.isNot = isNot;
        return this;
    }

    @Override
    public IColumn copy() {
        IColumn newColumn = ASTNodeFactory.getInstance().createColumn();
        newColumn.setColumnName(columName)
            .setAlias(alias)
            .setDataType(dataType)
            .setTableName(tableName)
            .setDistinct(isDistinct())
            .setIsNot(isNot);
        return newColumn;
    }

    @Override
    public int compareTo(Object o) {
        throw new NotSupportException();
    }

    @Override
    public void accept(PlanVisitor visitor) {
        visitor.visit(this);
    }

    // ================== hashcode/equals/toString不要随意改动=================

    /**
     * 这个方法不要被自动修改！ 在很多地方都有用到。
     */
    @Override
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
    @Override
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

    @Override
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
