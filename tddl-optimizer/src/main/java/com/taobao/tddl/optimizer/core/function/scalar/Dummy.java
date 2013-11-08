package com.taobao.tddl.optimizer.core.function.scalar;

import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.ISelectable.DATA_TYPE;
import com.taobao.tddl.optimizer.core.function.ScalarFunction;

/**
 * 假函数，不能参与任何运算。如果需要实现bdb的运算，需要额外的写实现放到map里，这个的作用就是mysql,直接发送下去的函数
 * 
 * @author Whisper
 */
public class Dummy extends ScalarFunction {

    public Comparable clientCompute(Object[] args, IFunction f) {
        return f;
    }

    public int getArgSize() {
        return 0;
    }

    public boolean isSingleton() {
        return false;
    }

    public DATA_TYPE getReturnType(IFunction f) {
        // shenxun :这里似乎必须返回一个类型。但如果是个假定的不去实现的函数，也不知道应该是什么类型的。暂时返回LONG_VAL看看
        return DATA_TYPE.LONG_VAL;
    }

    public void compute(Object[] args, IFunction f) {
    }

}
