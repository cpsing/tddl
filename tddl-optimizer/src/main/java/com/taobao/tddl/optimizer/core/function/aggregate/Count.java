package com.taobao.tddl.optimizer.core.function.aggregate;

import java.util.HashMap;
import java.util.Map;

import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.ISelectable.DATA_TYPE;
import com.taobao.tddl.optimizer.core.function.AggregateFunction;
import com.taobao.tddl.optimizer.exceptions.FunctionException;

/**
 * @author jianghang 2013-11-8 下午4:08:12
 * @since 5.1.0
 */
public class Count extends AggregateFunction {

    long count = 0;

    public Count(){
    }

    public void serverMap(Object[] args, IFunction f) throws FunctionException {
        count++;

    }

    public void serverReduce(Object[] args, IFunction f) throws FunctionException {
        count += ((Long) args[0]);

    }

    public int getArgSize() {
        return 1;
    }

    public Map<String, Object> getResult(IFunction f) {
        Map<String, Object> resMap = new HashMap<String, Object>();
        resMap.put(f.getName(), count);
        return resMap;
    }

    public void clear() {
        this.count = 0;
    }

    public DATA_TYPE getReturnType(IFunction f) {
        return DATA_TYPE.LONG_VAL;
    }

    public DATA_TYPE getMapReturnType(IFunction f) {
        return DATA_TYPE.LONG_VAL;
    }

}
