package com.taobao.tddl.optimizer.core.function.scalar;

import java.math.BigDecimal;
import java.math.BigInteger;

import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.expression.ISelectable.DATA_TYPE;
import com.taobao.tddl.optimizer.core.function.ScalarFunction;

/**
 * @since 5.1.0
 */
public class Sub extends ScalarFunction {

    public Comparable clientCompute(Object[] args, IFunction f) {
        for (Object arg : args) {
            if (arg instanceof ISelectable) return f;
        }

        return this.computeInner(args);
    }

    public int getArgSize() {
        return 2;
    }

    public void compute(Object[] args, IFunction f) {
        this.result = this.computeInner(args);

    }

    public DATA_TYPE getReturnType(IFunction f) {
        return DATA_TYPE.LONG_VAL;
    }

    private Comparable computeInner(Object[] args) {
        if (args == null || args.length == 0) {
            return null;
        }

        if (args.length == 1) {
            if (args[0] instanceof Long || args[0] instanceof Integer) {
                return 0 - ((Number) args[0]).longValue();
            } else if (args[0] instanceof Double || args[0] instanceof Float) {
                return 0 - ((Number) args[0]).doubleValue();
            } else if (args[0] instanceof BigDecimal) {
                BigDecimal o2 = new BigDecimal(args[0].toString());
                BigDecimal o = new BigDecimal(0);
                return o.subtract(o2);
            } else if (args[0] instanceof BigInteger) {
                BigInteger o2 = new BigInteger(args[0].toString());
                long o2l = o2.longValue();
                return 0 - o2l;
            }

            throw new NotSupportException("target " + args[0] + ". type " + args[0].getClass());
        } else {
            if (args[0] instanceof Long || args[0] instanceof Integer) {
                return ((Number) args[0]).longValue() - ((Number) args[1]).longValue();
            } else if (args[0] instanceof Double || args[0] instanceof Float) {
                return ((Number) args[0]).doubleValue() - ((Number) args[1]).doubleValue();
            } else if (args[0] instanceof BigDecimal || args[1] instanceof BigDecimal) {
                BigDecimal o = new BigDecimal(args[0].toString());
                BigDecimal o2 = new BigDecimal(args[1].toString());
                return o.subtract(o2);
            }

            throw new NotSupportException();
        }
    }
}
