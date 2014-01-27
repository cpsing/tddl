package com.taobao.tddl.optimizer.core.expression;

import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.expression.IFunction.FunctionType;

/**
 * 扩展函数实例，比如用于实现Merge的count/min等聚合函数
 * 
 * @since 5.0.0
 */
public interface IExtraFunction {

    /**
     * 设置function配置定义
     */
    public void setFunction(IFunction function);

    /**
     * Aggregate/Scalar函数
     */
    public FunctionType getFunctionType();

    /**
     * 获取Map函数的返回结果
     */
    public abstract DataType getMapReturnType();

    /**
     * 获取最后返回结果类型
     */
    public abstract DataType getReturnType();

    /**
     * 获取function的结果
     */
    public Object getResult();

    /**
     * 清除函数计算的中间结果，group by时使用
     */
    public void clear();
}
