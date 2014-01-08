package com.taobao.tddl.executor.function.scalar;

import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.expression.ISelectable.DATA_TYPE;

/**
 * @since 5.1.0
 */
public class Now extends ScalarFunction {

    public void compute(Object[] args) {
        result = new java.util.Date();
    }

    public DATA_TYPE getReturnType() {
        return DATA_TYPE.TIMESTAMP_VAL;
    }

}
