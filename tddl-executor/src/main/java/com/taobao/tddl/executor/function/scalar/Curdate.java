package com.taobao.tddl.executor.function.scalar;

import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * @since 5.0.0
 */
public class Curdate extends ScalarFunction {

    @Override
    public Object getResult() {
        return result;
    }

    @Override
    public void compute(Object[] args) {
        result = new java.sql.Date(new java.util.Date().getTime());
    }

    @Override
    public DataType getReturnType() {
        return DataType.DateType;
    }

}
