package com.taobao.tddl.executor.function.scalar;

import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.expression.ISelectable;

/**
 * @since 5.1.0
 */
public class Add extends ScalarFunction {

    private Object computeInner(Object[] args) {

        DataType type = this.getReturnType();

        Object v1 = type.convertFromObject(args[0]);
        Object v2 = type.convertFromObject(args[1]);

        return type.getCalculator().add(v1, v2);

    }

    @Override
    public void compute(Object[] args) {
        result = this.computeInner(args);
    }

    @Override
    public DataType getReturnType() {
        Object[] args = function.getArgs().toArray();
        DataType type = null;
        if (args[0] instanceof ISelectable) {
            type = ((ISelectable) args[0]).getDataType();
        }

        return type;
    }

}
