package com.taobao.tddl.executor.function;

import com.taobao.tddl.optimizer.core.expression.IExtraFunction;
import com.taobao.tddl.optimizer.core.expression.IFunction.FunctionType;

/**
 * 聚合函数
 * 
 * @author jianghang 2013-11-8 下午3:45:05
 * @since 5.0.0
 */
public abstract class AggregateFunction extends ExtraFunction implements IExtraFunction {

    public FunctionType getFunctionType() {
        return FunctionType.Aggregate;
    }

    public String getDbFunction() {
        return function.getColumnName();
    }

}
