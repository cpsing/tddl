package com.taobao.tddl.optimizer.core.function.scalar;

import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.expression.ISelectable.DATA_TYPE;
import com.taobao.tddl.optimizer.core.function.ScalarFunction;

/**
 * @author jianghang 2013-11-8 下午4:39:23
 * @since 5.1.0
 */
public class Not extends ScalarFunction {

    public int getArgSize() {
        return 1;
    }

    public Comparable clientCompute(Object[] args, IFunction f) {
        for (Object arg : args) {
            if (arg instanceof ISelectable) return f;
        }
        return this.computeInner(args);
    }

    public void compute(Object[] args, IFunction f) {
        result = this.computeInner(args);
    }

    public DATA_TYPE getReturnType(IFunction f) {
        return DATA_TYPE.BOOLEAN_VAL;
    }

    private Comparable computeInner(Object[] args) {
        if (args[0] instanceof Number) {
            return ((Number) args[0]).longValue() == 0 ? 1 : 0;
        }
        if (args[0] instanceof Boolean) {
            if (((Boolean) args[0])) {
                return false;
            } else {
                return true;
            }
        }

        throw new NotSupportException("Not Function");
    }

}
