package com.taobao.tddl.executor.function.aggregate;

import java.util.HashMap;
import java.util.Map;

import com.taobao.tddl.executor.function.AggregateFunction;
import com.taobao.tddl.optimizer.core.expression.ISelectable.DATA_TYPE;
import com.taobao.tddl.optimizer.exceptions.FunctionException;

/**
 * @since 5.1.0
 */
public class Count extends AggregateFunction {

    private long count = 0;

    public void serverMap(Object[] args) throws FunctionException {
        count++;
    }

    public void serverReduce(Object[] args) throws FunctionException {
        count += ((Long) args[0]);

    }

    public Map<String, Object> getResult() {
        Map<String, Object> resMap = new HashMap<String, Object>();
        resMap.put(function.getColumnName(), count);
        return resMap;
    }

    public void clear() {
        this.count = 0;
    }

    public DATA_TYPE getReturnType() {
        return DATA_TYPE.LONG_VAL;
    }

    public DATA_TYPE getMapReturnType() {
        return DATA_TYPE.LONG_VAL;
    }

}
