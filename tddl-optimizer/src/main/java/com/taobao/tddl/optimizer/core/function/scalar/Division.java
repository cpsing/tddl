package com.taobao.tddl.optimizer.core.function.scalar;

import java.math.BigDecimal;
import java.math.RoundingMode;

import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.expression.ISelectable.DATA_TYPE;
import com.taobao.tddl.optimizer.core.function.ScalarFunction;

/**
 * @since 5.1.0
 */
public class Division extends ScalarFunction {

    public Comparable clientCompute(Object[] args, IFunction f) {
        for (Object arg : args) {
            if (arg instanceof ISelectable) {
                return f;
            }
        }

        return computeInner(args);
    }

    public int getArgSize() {
        return 2;
    }

    public void compute(Object[] args, IFunction f) {
        this.result = computeInner(args);
    }

    private Comparable computeInner(Object[] args) {
        if (args[0] instanceof Long || args[0] instanceof Integer) {
            return ((Number) args[0]).longValue() * 1.0 / ((Number) args[1]).longValue() * 1.0;
        } else if (args[0] instanceof Double || args[0] instanceof Float) {
            return ((Number) args[0]).doubleValue() / ((Number) args[1]).doubleValue();
        } else if (args[0] instanceof String || args[1] instanceof String) {
            return Double.parseDouble(args[0].toString()) / Double.parseDouble(args[1].toString());
        } else if (args[0] instanceof BigDecimal) {
            BigDecimal o = new BigDecimal(args[0].toString());
            BigDecimal o2 = new BigDecimal(args[1].toString());
            return o.divide(o2, 4, RoundingMode.HALF_DOWN);
        } else {
            throw new NotSupportException("Division Type");
        }
    }

    public DATA_TYPE getReturnType(IFunction f) {
        return DATA_TYPE.DOUBLE_VAL;
    }

}
