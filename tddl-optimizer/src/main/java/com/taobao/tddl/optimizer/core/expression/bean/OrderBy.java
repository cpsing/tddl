package com.taobao.tddl.optimizer.core.expression.bean;

import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.PlanVisitor;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;

/**
 * @since 5.0.0
 */
public class OrderBy implements IOrderBy {

    private ISelectable column;

    /**
     * true : asc <br/>
     * false: desc
     */
    private Boolean     direction = true;

    @Override
    public IOrderBy setColumn(ISelectable columnName) {
        this.column = columnName;
        return this;
    }

    @Override
    public IOrderBy setDirection(Boolean direction) {
        this.direction = direction;
        return this;
    }

    @Override
    public ISelectable getColumn() {
        return column;
    }

    @Override
    public Boolean getDirection() {
        return direction;
    }

    @Override
    public IOrderBy assignment(Map<Integer, ParameterContext> parameterSettings) {
        IOrderBy newOrderBy = ASTNodeFactory.getInstance().createOrderBy();
        newOrderBy.setDirection(this.getDirection());
        if (this.getColumn() instanceof ISelectable) {
            newOrderBy.setColumn(this.getColumn().assignment(parameterSettings));
        } else {
            newOrderBy.setColumn(this.getColumn());
        }

        return newOrderBy;
    }

    @Override
    public void accept(PlanVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public String getAlias() {
        return this.column.getAlias();
    }

    @Override
    public IOrderBy setTableName(String alias) {
        this.column.setTableName(alias);
        return this;
    }

    @Override
    public IOrderBy setColumnName(String alias) {
        this.column.setColumnName(alias);
        return this;
    }

    @Override
    public String getTableName() {
        return this.column.getTableName();
    }

    @Override
    public String getColumnName() {
        return this.column.getColumnName();
    }

    @Override
    public DataType getDataType() {
        return this.column.getDataType();
    }

    @Override
    public IOrderBy copy() {
        IOrderBy newOrder = ASTNodeFactory.getInstance().createOrderBy();
        newOrder.setColumn(this.getColumn().copy());
        newOrder.setDirection(this.getDirection());
        return newOrder;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("OrderBy [");
        if (column != null) {
            builder.append("columnName=");
            builder.append(column);
            builder.append(", ");
        }
        builder.append("direction=");
        builder.append(direction);
        builder.append("]");
        return builder.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        final OrderBy other = (OrderBy) obj;
        if (this.column != other.column && (this.column == null || !this.column.equals(other.column))) {
            return false;
        }
        if (this.direction != other.direction) {
            return false;
        }
        return true;
    }

    @Override
    public String toStringWithInden(int inden) {
        StringBuilder sb = new StringBuilder();
        sb.append(column);
        if (!direction) {
            sb.append(" desc");
        }
        return sb.toString();
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 37 * hash + (this.column != null ? this.column.hashCode() : 0);
        hash = 37 * hash + (this.direction ? 1 : 0);
        return hash;
    }
}
