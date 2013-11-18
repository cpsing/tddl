package com.taobao.tddl.optimizer.core.function.scalar;

import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.ISelectable.DATA_TYPE;
import com.taobao.tddl.optimizer.core.function.ScalarFunction;

/**
 * @since 5.1.0
 */
public class IfNull extends ScalarFunction {

    public Comparable clientCompute(Object[] args, IFunction f) {
        return f;
    }

    public int getArgSize() {
        return 2;
    }

    public void compute(Object[] args, IFunction f) {
        if (args == null) {
            this.result = null;
        }

        if (args.length == 2) {
            if (args[0] == null) {
                this.result = args[1];
            } else {
                this.result = args[0];
            }
        }

    }

    public DATA_TYPE getReturnType(IFunction f) {
        Object[] args = f.getArgs().toArray();
        DATA_TYPE type = null;
        if (args[0] instanceof IColumn) {
            type = ((IColumn) args[0]).getDataType();
        }

        if (args[0] instanceof IFunction) {
            type = ((IFunction) args[0]).getDataType();
        }

        return type;
    }
}
