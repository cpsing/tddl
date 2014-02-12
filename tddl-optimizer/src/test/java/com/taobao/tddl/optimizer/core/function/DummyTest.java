package com.taobao.tddl.optimizer.core.function;

import org.junit.Ignore;

import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.expression.IExtraFunction;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.IFunction.FunctionType;
import com.taobao.tddl.optimizer.exceptions.FunctionException;

/**
 * 假函数，不能参与任何运算。如果需要实现bdb的运算，需要额外的写实现放到map里，这个的作用就是mysql,直接发送下去的函数
 * 
 * @author Whisper
 */
@Ignore
public class DummyTest implements IExtraFunction {

    public DataType getReturnType() {
        return null;
    }

    public void compute(Object[] args) throws FunctionException {
    }

    public void setFunction(IFunction function) {

    }

    public FunctionType getFunctionType() {
        return FunctionType.Scalar;
    }

    public DataType getMapReturnType() {
        return getReturnType();
    }

    public Object getResult() {
        return null;
    }

    public void clear() {
    }

}
