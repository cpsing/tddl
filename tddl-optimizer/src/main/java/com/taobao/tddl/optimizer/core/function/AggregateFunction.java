package com.taobao.tddl.optimizer.core.function;

import java.util.Map;

import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.IFunction.FunctionType;
import com.taobao.tddl.optimizer.exceptions.FunctionException;

/**
 * 聚合函数
 * 
 * @author jianghang 2013-11-8 下午3:45:05
 * @since 5.1.0
 */
public abstract class AggregateFunction implements IExtraFunction {

    public FunctionType getFunctionType() {
        return FunctionType.Aggregate;
    }

    public String getFunctionName() {
        return this.getClass().getSimpleName();
    }

    public Comparable clientCompute(Object[] args, IFunction f) throws FunctionException {
        throw new NotSupportException();
    }

    public boolean isSingleton() {
        return false;
    }

    public String getDbFunction(IFunction func) {
        return func.getColumnName();
    }

    public abstract Map<String, Object> getResult(IFunction f);
}
