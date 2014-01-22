package com.taobao.tddl.executor.function;

import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.expression.IExtraFunction;
import com.taobao.tddl.optimizer.exceptions.FunctionException;

/**
 * 假函数，不能参与任何运算。如果需要实现bdb的运算，需要额外的写实现放到map里，这个的作用就是mysql,直接发送下去的函数
 * 
 * @author Whisper
 */
public class Dummy extends ScalarFunction implements IExtraFunction {

    @Override
    public DataType getReturnType() {
        return null;
    }

    @Override
    public void compute(Object[] args) throws FunctionException {

    }

}
