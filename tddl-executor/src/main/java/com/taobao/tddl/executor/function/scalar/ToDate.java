package com.taobao.tddl.executor.function.scalar;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.exceptions.FunctionException;

/**
 * @since 5.0.0
 */
public class ToDate extends ScalarFunction {

    @Override
    public void compute(Object[] args) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        try {
            this.result = df.parse(args[0].toString());
        } catch (ParseException e) {
            throw new FunctionException(e);
        }
    }

    @Override
    public DataType getReturnType() {
        return DataType.DateType;
    }

}
