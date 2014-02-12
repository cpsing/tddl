package com.taobao.tddl.executor.function.scalar;

import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.datatype.DataTypeUtil;
import com.taobao.tddl.optimizer.core.expression.ISelectable;

/**
 * @since 5.0.0
 */
public class Division extends ScalarFunction {

    @Override
    public void compute(Object[] args) {
        this.result = computeInner(args);
    }

    private Object computeInner(Object[] args) {
        DataType type = this.getReturnType();
        return type.getCalculator().divide(args[0], args[1]);
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
        if (type == DataType.BigIntegerType) {
            // 如果是大整数，返回bigDecimal
            return DataType.BigDecimalType;
        } else {
            // 尽可能都返回为BigDecimalType，double类型容易出现精度问题，会和mysql出现误差
            // [zhuoxue.yll, 2516885.8000]
            // [zhuoxue.yll, 2516885.799999999813735485076904296875]
            // return DataType.DoubleType;
            return DataType.BigDecimalType;
        }
    }

}
