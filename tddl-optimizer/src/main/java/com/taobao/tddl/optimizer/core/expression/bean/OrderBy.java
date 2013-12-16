package com.taobao.tddl.optimizer.core.expression.bean;

import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.PlanVisitor;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.expression.ISelectable.DATA_TYPE;

/**
 * @since 5.1.0
 */
public class OrderBy implements IOrderBy {

    private ISelectable column;

    /**
     * true : asc <br/>
     * false: desc
     */
    private Boolean     direction = true;

    public IOrderBy setColumn(ISelectable columnName) {
        this.column = columnName;
        return this;
    }

    public IOrderBy setDirection(Boolean direction) {
        this.direction = direction;
        return this;
    }

    public ISelectable getColumn() {
        return column;
    }

    public Boolean getDirection() {
        return direction;
    }

    public IOrderBy assignment(Map<Integer, ParameterContext> parameterSettings) {
        IOrderBy newOrderBy = ASTNodeFactory.getInstance().createOrderBy();
        newOrderBy.setDirection(this.getDirection());
        if (this.getColumn() instanceof ISelectable) {
            newOrderBy.setColumn(((ISelectable) this.getColumn()).assignment(parameterSettings));
        } else {
            newOrderBy.setColumn(this.getColumn());
        }

        return newOrderBy;
    }

    public void accept(PlanVisitor visitor) {
        visitor.visit(this);
    }

    public String getAlias() {
        return this.column.getAlias();
    }

    public IOrderBy setTableName(String alias) {
        this.column.setTableName(alias);
        return this;
    }

    public IOrderBy setColumnName(String alias) {
        this.column.setColumnName(alias);
        return this;
    }

    public String getTableName() {
        return this.column.getTableName();
    }

    public String getColumnName() {
        return this.column.getColumnName();
    }

    public DATA_TYPE getDataType() {
        return this.column.getDataType();
    }

    public IOrderBy copy() {
        IOrderBy newOrder = ASTNodeFactory.getInstance().createOrderBy();
        newOrder.setColumn(this.getColumn().copy());
        newOrder.setDirection(this.getDirection());
        return newOrder;
    }

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

    public String toStringWithInden(int inden) {
        StringBuilder sb = new StringBuilder();
        sb.append(column);
        if (!direction) {
            sb.append(" desc");
        }
        return sb.toString();
    }

    public int hashCode() {
        int hash = 5;
        hash = 37 * hash + (this.column != null ? this.column.hashCode() : 0);
        hash = 37 * hash + (this.direction ? 1 : 0);
        return hash;
    }
}
