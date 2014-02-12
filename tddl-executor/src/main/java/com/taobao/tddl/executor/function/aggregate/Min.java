package com.taobao.tddl.executor.function.aggregate;

import com.taobao.tddl.executor.function.AggregateFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.datatype.DataTypeUtil;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.exceptions.FunctionException;

/**
 * @since 5.0.0
 */
public class Min extends AggregateFunction {

    public Min(){
    }

    @Override
    public void serverMap(Object[] args) throws FunctionException {
        doMin(args);

    }

    @Override
    public void serverReduce(Object[] args) throws FunctionException {
        doMin(args);
    }

    private void doMin(Object[] args) {
        Object o = args[0];

        DataType type = this.getReturnType();
        if (o != null) {
            if (result == null) {
                result = o;
            }

            if (type.compare(o, result) < 0) {
                result = o;
            }
        }
    }

    public int getArgSize() {
        return 1;
    }

    @Override
    public DataType getReturnType() {
        return this.getMapReturnType();
    }

    @Override
    public DataType getMapReturnType() {
        Object[] args = function.getArgs().toArray();

        DataType type = null;
        if (args[0] instanceof ISelectable) {
            type = ((ISelectable) args[0]).getDataType();
        }
        if (type == null) {
            type = DataTypeUtil.getTypeOfObject(args[0]);
        }
        return type;
    }

}
