package com.taobao.tddl.executor.function.scalar;

import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.ISelectable.DATA_TYPE;

/**
 * @since 5.1.0
 */
public class IfNull extends ScalarFunction {

    public void compute(Object[] args) {
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

    public DATA_TYPE getReturnType() {
        Object[] args = function.getArgs().toArray();
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
