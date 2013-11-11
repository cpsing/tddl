package com.taobao.tddl.optimizer.core.function.scalar;

import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.ISelectable.DATA_TYPE;
import com.taobao.tddl.optimizer.core.function.ScalarFunction;

public class Round extends ScalarFunction {

    public Comparable clientCompute(Object[] args, IFunction f) {
        return f;
    }

    public int getArgSize() {
        return 1;
    }

    public void compute(Object[] args, IFunction f) {
        if (args.length == 1) {
            if (args[0] instanceof Float) {
                this.result = Math.round((Float) args[0]);
            } else if (args[0] instanceof Double) {
                this.result = Math.round((Double) args[0]);
            } else {
                this.result = args[0];
            }
        } else {
            this.result = args[0];
        }

    }

    public DATA_TYPE getReturnType(IFunction f) {
        return DATA_TYPE.LONG_VAL;
    }

}
