package com.taobao.tddl.executor.function.scalar;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.expression.ISelectable.DATA_TYPE;
import com.taobao.tddl.optimizer.exceptions.FunctionException;

/**
 * @since 5.1.0
 */
public class ToDate extends ScalarFunction {

    public void compute(Object[] args) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        try {
            this.result = df.parse(args[0].toString());
        } catch (ParseException e) {
            throw new FunctionException(e);
        }
    }

    public DATA_TYPE getReturnType() {
        return DATA_TYPE.DATE_VAL;
    }

}
