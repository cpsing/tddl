package com.taobao.tddl.executor.function.aggregate;

import java.util.HashMap;
import java.util.Map;

import com.taobao.tddl.executor.function.AggregateFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.exceptions.FunctionException;

/**
 * @since 5.1.0
 */
public class Count extends AggregateFunction {

    private long count = 0;

    @Override
    public void serverMap(Object[] args) throws FunctionException {
        count++;
    }

    @Override
    public void serverReduce(Object[] args) throws FunctionException {
        count += ((Long) args[0]);

    }

    @Override
    public Map<String, Object> getResult() {
        Map<String, Object> resMap = new HashMap<String, Object>();
        resMap.put(function.getColumnName(), count);
        return resMap;
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
