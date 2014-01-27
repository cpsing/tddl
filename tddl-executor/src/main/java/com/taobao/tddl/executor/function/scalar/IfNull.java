package com.taobao.tddl.executor.function.scalar;

import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFunction;

/**
 * @since 5.0.0
 */
public class IfNull extends ScalarFunction {

    @Override
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

    @Override
    public DataType getReturnType() {
        Object[] args = function.getArgs().toArray();
        DataType type = null;
        if (args[0] instanceof IColumn) {
            type = ((IColumn) args[0]).getDataType();
        }

        if (args[0] instanceof IFunction) {
            type = ((IFunction) args[0]).getDataType();
        }

        return type;
    }
}
