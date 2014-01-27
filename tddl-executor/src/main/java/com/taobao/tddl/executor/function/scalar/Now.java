package com.taobao.tddl.executor.function.scalar;

import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * @since 5.0.0
 */
public class Now extends ScalarFunction {

    @Override
    public void compute(Object[] args) {
        result = new java.util.Date();
    }

    @Override
    public DataType getReturnType() {
        return DataType.TimestampType;
    }

}
