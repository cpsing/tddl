package com.taobao.tddl.optimizer.core.function.scalar;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.ISelectable.DATA_TYPE;
import com.taobao.tddl.optimizer.core.function.ScalarFunction;
import com.taobao.tddl.optimizer.exceptions.FunctionException;

/**
 * @since 5.1.0
 */
public class ToDate extends ScalarFunction {

    public Comparable clientCompute(Object[] args, IFunction f) {
        try {
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            return df.parse(args[0].toString());
        } catch (Exception e) {
            return f;
        }
    }

    public int getArgSize() {
        return 1;
    }

    public void compute(Object[] args, IFunction f) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        try {
            this.result = df.parse(args[0].toString());
        } catch (ParseException e) {
            throw new FunctionException(e);
        }
    }

    public DATA_TYPE getReturnType(IFunction f) {
        return DATA_TYPE.DATE_VAL;
    }

}
