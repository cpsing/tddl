package com.taobao.tddl.executor.function.scalar;

import java.math.BigDecimal;

import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.expression.ISelectable.DATA_TYPE;

public class Multiply extends ScalarFunction {

    public void compute(Object[] args) {
        this.result = computeInner(args);
    }

    public DATA_TYPE getReturnType() {
        return DATA_TYPE.LONG_VAL;
    }

    private Comparable computeInner(Object[] args) {
        if (args[0] instanceof Long || args[0] instanceof Integer) {
            return ((Number) args[0]).longValue() * ((Number) args[1]).longValue();
        } else if (args[0] instanceof Double || args[0] instanceof Float) {
            return ((Number) args[0]).doubleValue() * ((Number) args[1]).doubleValue();
        } else if (args[0] instanceof BigDecimal || args[1] instanceof BigDecimal) {
            BigDecimal o = new BigDecimal(args[0].toString());
            BigDecimal o2 = new BigDecimal(args[1].toString());
            return o.multiply(o2);
        }
        throw new IllegalArgumentException("not supported yet");
    }
}
