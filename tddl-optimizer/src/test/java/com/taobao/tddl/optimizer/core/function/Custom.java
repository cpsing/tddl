package com.taobao.tddl.optimizer.core.function;

import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.IFunction.FunctionType;
import com.taobao.tddl.optimizer.core.expression.ISelectable.DATA_TYPE;
import com.taobao.tddl.optimizer.exceptions.FunctionException;

/**
 * 只是一个mock实例
 * 
 * @author jianghang 2013-11-8 下午8:16:37
 * @since 5.1.0
 */
public class Custom implements IExtraFunction {

    public Comparable clientCompute(Object[] args, IFunction f) throws FunctionException {

        return null;
    }

    public void serverMap(Object[] args, IFunction f) throws FunctionException {

    }

    public void serverReduce(Object[] args, IFunction f) throws FunctionException {

    }

    public int getArgSize() {

        return 0;
    }

    public String getFunctionName() {

        return null;
    }

    public boolean isSingleton() {

        return false;
    }

    public Object getResult(IFunction f) {

        return null;
    }

    public FunctionType getFunctionType() {

        return null;
    }

    public DATA_TYPE getReturnType(IFunction f) {

        return null;
    }

    public DATA_TYPE getMapReturnType(IFunction f) {

        return null;
    }

    public void clear() {

    }

}
