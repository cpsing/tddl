package com.taobao.tddl.optimizer.core.function.scalar;

import java.math.BigDecimal;

import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.expression.ISelectable.DATA_TYPE;
import com.taobao.tddl.optimizer.core.function.ScalarFunction;

/**
 * @author jianghang 2013-11-8 下午4:19:56
 * @since 5.1.0
 */
public class Add extends ScalarFunction {

    public int getArgSize() {
        return 2;
    }

    public Comparable clientCompute(Object[] args, IFunction f) {
        for (Object arg : args) {
            if (arg instanceof ISelectable) {
                return f;
            }
        }
        return this.computeInner(args, f, true);
    }

    private Comparable computeInner(Object[] args, IFunction f, boolean fromClient) {
        if (args[0] instanceof Long || args[0] instanceof Integer) {
            return ((Number) args[0]).longValue() + ((Number) args[1]).longValue();
        } else if (args[0] instanceof Double || args[0] instanceof Float) {
            return ((Number) args[0]).doubleValue() + ((Number) args[1]).doubleValue();
        } else if (args[0] instanceof BigDecimal || args[1] instanceof BigDecimal) {
            BigDecimal o = new BigDecimal(args[0].toString());
            BigDecimal o2 = new BigDecimal(args[1].toString());

            return o.add(o2);
        }
        if (fromClient) {
            return f;
        } else {
            throw new IllegalArgumentException("not supported yet");
        }
    }

    public void compute(Object[] args, IFunction f) {
        result = this.computeInner(args, f, false);
    }

    public DATA_TYPE getReturnType(IFunction f) {
        return DATA_TYPE.LONG_VAL;
    }

}
