package com.taobao.tddl.executor.function.aggregate;

import com.taobao.tddl.executor.function.AggregateFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.datatype.DataTypeUtil;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.exceptions.FunctionException;

/**
 * @since 5.0.0
 */
public class Sum extends AggregateFunction {

    public Sum(){
    }

    @Override
    public void serverMap(Object[] args) throws FunctionException {
        doSum(args);
    }

    @Override
    public void serverReduce(Object[] args) throws FunctionException {
        doSum(args);
    }

    private void doSum(Object[] args) {
        Object o = args[0];
        DataType type = this.getMapReturnType();

        if (result == null) {
            o = type.convertFrom(o);
            result = o;
        } else {
            result = type.getCalculator().add(result, o);
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
        if (type == DataType.IntegerType || type == DataType.ShortType) {
            return DataType.LongType;
        } else {
            return type;
        }
    }

}
