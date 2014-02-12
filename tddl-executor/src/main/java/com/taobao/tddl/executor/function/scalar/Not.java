package com.taobao.tddl.executor.function.scalar;

import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.datatype.DataTypeUtil;
import com.taobao.tddl.optimizer.core.expression.ISelectable;

/**
 * @since 5.0.0
 */
public class Not extends ScalarFunction {

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
        if (type == null) {
            type = DataTypeUtil.getTypeOfObject(args[0]);
        }
        if (type == DataType.BooleanType) {
            return DataType.BooleanType;
        } else {
            return DataType.IntegerType;
        }
    }

    private Object computeInner(Object[] args) {
        DataType type = getReturnType();
        Object obj = type.convertFrom(args[0]);
        if (obj instanceof Number) {
            return ((Number) args[0]).longValue() == 0 ? 1 : 0;
        } else if (obj instanceof Boolean) {
            return obj;
        } else {
            // 非数字类型全为false，一般不会走到这逻辑
            return false;
        }
    }

}
