package com.taobao.tddl.executor.function.scalar;

import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.expression.ISelectable.DATA_TYPE;

/**
 * @since 5.1.0
 */
public class Not extends ScalarFunction {

    public void compute(Object[] args) {
        result = this.computeInner(args);
    }

    public DATA_TYPE getReturnType() {
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
