package com.taobao.tddl.executor.function.scalar;

import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.datatype.DataTypeUtil;
import com.taobao.tddl.optimizer.core.expression.ISelectable;

/**
 * 对应mysql的bitor '|'
 * 
 * @author jianghang 2014-2-13 上午11:55:29
 * @since 5.0.0
 */
public class BitOr extends ScalarFunction {

    @Override
    public void compute(Object[] args) {
        this.result = computeInner(args);
    }

    private Object computeInner(Object[] args) {
        DataType type = this.getReturnType();
        return type.getCalculator().bitOr(args[0], args[1]);
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

        if (type == DataType.BigIntegerType || type == DataType.BigDecimalType) {
            return DataType.BigIntegerType;
        } else {
            return DataType.LongType;
        }
    }

}
