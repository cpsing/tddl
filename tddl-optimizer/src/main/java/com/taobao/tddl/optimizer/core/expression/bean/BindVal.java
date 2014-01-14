package com.taobao.tddl.optimizer.core.expression.bean;

import java.util.Map;

import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.optimizer.core.PlanVisitor;
import com.taobao.tddl.optimizer.core.expression.IBindVal;

/**
 * 绑定变量
 * 
 * @author Whisper
 */
public class BindVal implements IBindVal {

    private final int index;

    public BindVal(int index){
        this.index = index;
    }

    @Override
    public int compareTo(Object o) {
        throw new NotSupportException();
    }

    @Override
    public Object assignment(Map<Integer, ParameterContext> parameterSettings) {
        ParameterContext paramContext = parameterSettings.get(index);
        if (paramContext == null) {
            throw new TddlRuntimeException("can't find param by index :" + index + " ." + "context : "
                                           + parameterSettings);
        }

        if (paramContext.getArgs()[1] == null) {
            return NullValue.getNullValue();
        }

        return paramContext.getArgs()[1];
    }

    @Override
    public String toString() {
        return "BindVal [index=" + index + "]";
    }

    public int getBindVal() {
        return index;
    }

    @Override
    public void accept(PlanVisitor visitor) {
        visitor.visit(this);
    }

}
