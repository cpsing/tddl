package com.taobao.tddl.executor.function.scalar;

import java.math.BigDecimal;

import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * @since 5.1.0
 */
public class Add extends ScalarFunction {

    private Comparable computeInner(Object[] args) {
        if (args[0] instanceof Long || args[0] instanceof Integer) {
            return ((Number) args[0]).longValue() + ((Number) args[1]).longValue();
        } else if (args[0] instanceof Double || args[0] instanceof Float) {
            return ((Number) args[0]).doubleValue() + ((Number) args[1]).doubleValue();
        } else if (args[0] instanceof BigDecimal || args[1] instanceof BigDecimal) {
            BigDecimal o = new BigDecimal(args[0].toString());
            BigDecimal o2 = new BigDecimal(args[1].toString());

            return o.add(o2);
        }
        throw new IllegalArgumentException("not supported yet");
    }

    @Override
    public void compute(Object[] args) {
        result = this.computeInner(args);
    }

    @Override
    public DataType getReturnType() {
        return DataType.LongType;
    }

}
