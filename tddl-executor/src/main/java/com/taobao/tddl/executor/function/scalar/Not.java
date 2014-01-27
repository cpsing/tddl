package com.taobao.tddl.executor.function.scalar;

import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * @since 5.0.0
 */
public class Not extends ScalarFunction {

    @Override
    public void compute(Object[] args) {
        result = this.computeInner(args);
    }

    @Override
    public DataType getReturnType() {
        return DataType.BooleanType;
    }

    private Object computeInner(Object[] args) {
        DataType type = getReturnType();
        return type.convertFrom(args[0]);
    }

}
