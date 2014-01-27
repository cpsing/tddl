package com.taobao.tddl.optimizer.core.expression.bean;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.PlanVisitor;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.ILogicalFilter;
import com.taobao.tddl.optimizer.utils.OptimizerToString;

/**
 * @since 5.0.0
 */
public class LogicalFilter extends Function<ILogicalFilter> implements ILogicalFilter {

    protected boolean   evaled = false;
    protected OPERATION operation;

    public LogicalFilter(){
        args = new ArrayList<IFilter>();
    }

    public ILogicalFilter setOperation(OPERATION operation) {
        this.operation = operation;
        this.functionName = operation.getOPERATIONString();
        return this;
    }

    public OPERATION getOperation() {
        return operation;
    }

    public List<IFilter> getSubFilter() {
        return args;
    }

    public ILogicalFilter setSubFilter(List<IFilter> subFilters) {
        this.setArgs(subFilters);
        return this;
    }

    public IFilter getLeft() {
        if (this.args.isEmpty()) {
            return null;
        }

        return (IFilter) this.args.get(0);
    }

    public IFilter getRight() {
        if (this.args.size() < 2) {
            return null;
        }

        return (IFilter) args.get(1);
    }

    public IFilter setLeft(IFilter left) {
        if (this.args.isEmpty()) {
            this.args.add(left);
        } else {
            this.args.set(0, left);
        }
        return this;
    }

    public IFilter setRight(IFilter Right) {
        if (this.args.isEmpty()) {
            this.args.add(null);
            this.args.add(Right);
        } else if (this.args.size() == 1) {
            this.args.add(Right);
        } else {
            this.args.set(1, Right);

        }
        return this;
    }

    public ILogicalFilter addSubFilter(IFilter subFilter) {
        this.args.add(subFilter);
        return this;
    }

    public ILogicalFilter assignment(Map<Integer, ParameterContext> parameterSettings) {
        ILogicalFilter filterNew = (ILogicalFilter) super.assignment(parameterSettings);
        filterNew.setOperation(this.getOperation());
        return filterNew;
    }

    public String toString() {
        return OptimizerToString.printFilterString(this);
    }

    public ILogicalFilter copy() {
        ILogicalFilter filterNew = ASTNodeFactory.getInstance().createLogicalFilter();
        super.copy(filterNew);
        filterNew.setOperation(this.getOperation());
        return filterNew;
    }

    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((operation == null) ? 0 : operation.hashCode());
        result = prime * result + ((args == null) ? 0 : args.hashCode());
        return result;
    }

    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof ILogicalFilter)) {
            return false;
        }
        ILogicalFilter other = (ILogicalFilter) obj;
        if (getOperation() != other.getOperation()) {
            return false;

        }
        if (getSubFilter() == null) {
            if (other.getSubFilter() != null) {
                return false;

            }
        } else if (!getSubFilter().equals(other.getSubFilter())) {
            return false;
        }
        return true;
    }

    public void accept(PlanVisitor visitor) {
        visitor.visit(this);
    }

}
