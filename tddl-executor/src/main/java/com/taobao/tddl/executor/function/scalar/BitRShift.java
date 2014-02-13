package com.taobao.tddl.executor.function.scalar;

import java.math.BigInteger;

import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * <pre>
 * Shifts a longlong (BIGINT) number to the right.
 * 
 * mysql> SELECT 4 >> 2;
 *         -> 1
 * </pre>
 * 
 * @author jianghang 2014-2-13 下午1:13:06
 * @since 5.0.0
 */
public class BitRShift extends ScalarFunction {

    @Override
    public void compute(Object[] args) {
        this.result = computeInner(args);
    }

    private Object computeInner(Object[] args) {
        BigInteger o1 = DataType.BigIntegerType.convertFrom(args[0]);
        Integer o2 = (Integer) DataType.IntegerType.convertFrom(args[1]);
        return o1.shiftRight(o2);
    }

    @Override
    public DataType getReturnType() {
        return DataType.BigIntegerType;
    }

}
