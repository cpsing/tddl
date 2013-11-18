package com.taobao.tddl.optimizer.core.function.scalar;

import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.ISelectable.DATA_TYPE;
import com.taobao.tddl.optimizer.core.function.ScalarFunction;

/**
 * @since 5.1.0
 */
public class Curdate extends ScalarFunction {

    public Comparable clientCompute(Object[] args, IFunction f) {
        return new java.sql.Date(new java.util.Date().getTime());
    }

    public int getArgSize() {
        return 0;
    }

    public Object getResult(IFunction f) {
        return result;
    }

    public void compute(Object[] args, IFunction f) {
        result = new java.sql.Date(new java.util.Date().getTime());
    }

    public DATA_TYPE getReturnType(IFunction f) {
        return DATA_TYPE.DATE_VAL;
    }

}
