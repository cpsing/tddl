package com.taobao.tddl.executor.function.aggregate;

import com.taobao.tddl.executor.function.AggregateFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.exceptions.FunctionException;

/**
 * @since 5.0.0
 */
public class Count extends AggregateFunction {

    private long count = 0;

    @Override
    public void serverMap(Object[] args) throws FunctionException {
        count++;
    }

    @Override
    public void serverReduce(Object[] args) throws FunctionException {
        DataType type = this.getReturnType();
        Object o = args[0];
        if (o != null) {
            count += (Long) type.convertFrom(o);
        }
    }

    @Override
    public Object getResult() {
        return count;
    }

    @Override
    public void clear() {
        this.count = 0;
    }

    @Override
    public DataType getReturnType() {
        return DataType.LongType;
    }

    @Override
    public DataType getMapReturnType() {
        return DataType.LongType;
    }

}
