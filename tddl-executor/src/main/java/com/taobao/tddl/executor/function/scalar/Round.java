package com.taobao.tddl.executor.function.scalar;

import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;

public class Round extends ScalarFunction {

    @Override
    public void compute(Object[] args) {
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

    @Override
    public DataType getReturnType() {
        return DataType.LongType;
    }

}
