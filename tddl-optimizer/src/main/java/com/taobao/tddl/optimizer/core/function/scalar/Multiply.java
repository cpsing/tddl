package com.taobao.tddl.optimizer.core.function.scalar;

import java.math.BigDecimal;

import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.expression.ISelectable.DATA_TYPE;
import com.taobao.tddl.optimizer.core.function.ScalarFunction;

public class Multiply extends ScalarFunction {

    public Comparable clientCompute(Object[] args, IFunction f) {
        for (Object arg : args) {
            if (arg instanceof ISelectable) return f;
        }
        return computeInner(args);
    }

    public int getArgSize() {
        return 2;
    }

    public void compute(Object[] args, IFunction f) {
        this.result = computeInner(args);
    }

    public DATA_TYPE getReturnType(IFunction f) {
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
