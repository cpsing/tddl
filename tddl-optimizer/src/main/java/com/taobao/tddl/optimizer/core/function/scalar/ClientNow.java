package com.taobao.tddl.optimizer.core.function.scalar;

import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.ISelectable.DATA_TYPE;
import com.taobao.tddl.optimizer.core.function.ScalarFunction;
import com.taobao.tddl.optimizer.exceptions.FunctionException;

/**
 * @since 5.1.0
 */
public class ClientNow extends ScalarFunction {

    public Comparable clientCompute(Object[] args, IFunction f) {
        return new java.util.Date().getTime();
    }

    public int getArgSize() {
        return 0;
    }

    public void compute(Object[] args, IFunction f) {
        throw new FunctionException("这不科学！");
    }

    public DATA_TYPE getReturnType(IFunction f) {
        return DATA_TYPE.LONG_VAL;
    }

}
