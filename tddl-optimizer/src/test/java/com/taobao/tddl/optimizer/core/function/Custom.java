package com.taobao.tddl.optimizer.core.function;

import com.taobao.tddl.optimizer.core.expression.IExtraFunction;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.IFunction.FunctionType;
import com.taobao.tddl.optimizer.core.expression.ISelectable.DATA_TYPE;

/**
 * 只是一个mock实例
 * 
 * @author jianghang 2013-11-8 下午8:16:37
 * @since 5.1.0
 */
public class Custom implements IExtraFunction {

    public void setFunction(IFunction function) {

    }

    public FunctionType getFunctionType() {

        return null;
    }

    public DATA_TYPE getMapReturnType() {

        return null;
    }

    public DATA_TYPE getReturnType() {

        return null;
    }

    public Object getResult() {

        return null;
    }

    public void clear() {

    }

}
