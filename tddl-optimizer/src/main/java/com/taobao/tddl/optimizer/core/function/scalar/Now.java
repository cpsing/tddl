package com.taobao.tddl.optimizer.core.function.scalar;

import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.ISelectable.DATA_TYPE;
import com.taobao.tddl.optimizer.core.function.ScalarFunction;

/**
 * @author jianghang 2013-11-8 下午4:39:19
 * @since 5.1.0
 */
public class Now extends ScalarFunction {

    public Comparable clientCompute(Object[] args, IFunction f) {
        return f;
    }

    public int getArgSize() {
        return 0;
    }

    public void compute(Object[] args, IFunction f) {
        result = new java.util.Date();
    }

    public DATA_TYPE getReturnType(IFunction f) {
        return DATA_TYPE.TIMESTAMP;
    }

}
