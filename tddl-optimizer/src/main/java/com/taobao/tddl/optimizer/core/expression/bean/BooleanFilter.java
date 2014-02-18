package com.taobao.tddl.optimizer.core.expression.bean;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.PlanVisitor;
import com.taobao.tddl.optimizer.core.expression.IBindVal;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.utils.OptimizerToString;

/**
 * @since 5.0.0
 */
public class BooleanFilter extends Function<IBooleanFilter> implements IBooleanFilter {

    protected OPERATION operation;

    public BooleanFilter(){
        this.args.add(null);
        this.args.add(null);
    }

    @Override
    public OPERATION getOperation() {
        return operation;
    }

    @Override
    public IBooleanFilter setOperation(OPERATION operation) {
        if (operation == OPERATION.AND || operation == OPERATION.OR) {
            throw new NotSupportException(" and or is not support ");
        }

        this.operation = operation;
        this.functionName = operation.getOPERATIONString();
        return this;
    }

    @Override
    public Object getColumn() {
        return this.args.get(0);
    }

    @Override
    public IBooleanFilter setColumn(Object column) {
        this.args.set(0, column);
        return this;
    }

    @Override
    public Object getValue() {
        if (this.args.get(1) instanceof List) {
            return null;
        } else {
            return this.args.get(1);
        }
    }

    @Override
    public List<Object> getValues() {
        if (this.args.get(1) instanceof List) {
            return (List) this.args.get(1);
        } else {
            return null;
        }
    }

    @Override
    public IBooleanFilter setValues(List values) {
        this.args.set(1, values);
        return this;
    }

    @Override
    public IBooleanFilter setValue(Object value) {
        this.args.set(1, value);
        return this;
    }

    @Override
    public String toString() {
        return OptimizerToString.printFilterString(this);
    }

    @Override
    public IBooleanFilter copy() {
        IBooleanFilter filterNew = ASTNodeFactory.getInstance().createBooleanFilter();
        super.copy(filterNew);
        filterNew.setOperation(this.getOperation());
        return filterNew;
    }

    @Override
    public IBooleanFilter assignment(Map<Integer, ParameterContext> parameterSettings) {
        IBooleanFilter bf = super.assignment(parameterSettings);
        bf.setOperation(this.getOperation());

        if (bf.getValues() != null) {
            List<Object> vals = getValues();
            List<Object> newVals = new ArrayList(vals.size());
            for (Object val : vals) {
                if (val instanceof IBindVal) {
                    newVals.add(((IBindVal) val).assignment(parameterSettings));
                } else {
                    newVals.add(val);
                }
            }

            this.setValues(newVals);
        } else if (bf.getValue() != null) {
            Object val = bf.getValue();
            if (val instanceof IBindVal) {
                val = ((IBindVal) val).assignment(parameterSettings);
            }

            this.setValue(val);
        }
        return bf;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((operation == null) ? 0 : operation.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof IBooleanFilter)) {
            return false;
        }
        IBooleanFilter other = (IBooleanFilter) obj;
        if (getOperation() != other.getOperation()) {
            return false;
        }
        if (getColumn() == null) {
            if (other.getColumn() != null) {
                return false;
            }
        } else if (!getColumn().equals(other.getColumn())) {
            return false;
        }

        if (getValue() == null) {
            if (other.getValue() != null) {
                return false;
            }
        } else if (!getValue().equals(other.getValue())) {
            return false;
        }

        if (getValues() == null) {
            if (other.getValues() != null) {
                return false;
            }
        } else if (!getValues().equals(other.getValues())) {
            return false;
        }

        return true;
    }

    @Override
    public String getColumnName() {
        if (this.columnName == null || this.columnName.isEmpty()) {
            return TStringUtil.upperCase(OptimizerToString.printFilterString(this, 0, false));
        } else {// 在builder阶段，为了保证节点间相互关系，所以可能会对columnName进行重新的定义。
            return columnName;
        }
    }

    @Override
    public void accept(PlanVisitor visitor) {
        visitor.visit(this);
    }
}
