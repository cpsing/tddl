package com.taobao.tddl.optimizer.core.function;

import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.IFunction.FunctionType;
import com.taobao.tddl.optimizer.core.expression.ISelectable.DATA_TYPE;
import com.taobao.tddl.optimizer.exceptions.FunctionException;

/**
 * 外部可计算函数，可直接计算
 * 
 * @author whisper
 * @author jianghang 2013-11-8 下午3:20:20
 * @since 5.1.0
 */
public interface IExtraFunction {

    /**
     * 用于标记数据在client端进行计算。
     * 
     * @param args
     * @param f
     * @return
     * @throws FunctionException
     */
    Comparable clientCompute(Object[] args, IFunction f) throws FunctionException;

    /**
     * 用于标记数据应该在server端进行计算。
     * 
     * <pre>
     * 因为server有分发的过程，所以这里也模拟了分发过程。 
     * 比如：
     * 1. sum会先map到所有相关的机器上
     * 2. reduce方法内做合并
     * 
     * 比较特殊的是avg
     * 1. 首先它在map的时候，需要下面的节点统计count + sum.
     * 2. reduce则是进行avg计算的地方，进行count/sum处理
     * 
     * 因为有map/reduce模型，所有带有函数计算的执行计划都被设定为： merge { query } 结构，也就是merge下面挂query的模型，
     * 
     * </pre>
     * 
     * @param args
     * @param f
     * @throws FunctionException
     */
    void serverMap(Object[] args, IFunction f) throws FunctionException;

    /**
     * 用于标记数据应该在server端进行计算。
     * 
     * <pre>
     * 因为server有分发的过程，所以这里也模拟了分发过程。 
     * 比如：
     * 1. sum会先map到所有相关的机器上
     * 2. reduce方法内做合并
     * 
     * 比较特殊的是avg
     * 1. 首先它在map的时候，需要下面的节点统计count + sum.
     * 2. reduce则是进行avg计算的地方，进行count/sum处理
     * 
     * 因为有map/reduce模型，所有带有函数计算的执行计划都被设定为： merge { query } 结构，也就是merge下面挂query的模型，
     * 
     * </pre>
     * 
     * @param args
     * @param f
     * @throws FunctionException
     */
    void serverReduce(Object[] args, IFunction f) throws FunctionException;

    /**
     * 参数个数。 如果返回-1则不限制参数个数
     * 
     * @return
     */
    int getArgSize();

    /**
     * 获取函数的名字
     * 
     * @return
     */
    public String getFunctionName();

    /**
     * 用于标记函数是否是个单例函数 比如now()这个函数，就不能是单例函数 而 add() 这个函数，就必须是单例函数
     * 
     * @return
     */
    public boolean isSingleton();

    /**
     * 获取当前函数的结果。
     * 
     * @param f
     * @return
     */
    Object getResult(IFunction f);

    /**
     * 获取函数的类型， 目前主要由两种类型 ， 一个是计算类，一个是聚合类(aggregate,比如sum count max min )
     * 
     * @return
     */
    FunctionType getFunctionType();

    public DATA_TYPE getReturnType(IFunction f);

    public DATA_TYPE getMapReturnType(IFunction f);

    /**
     * 如果可以用db的函数，那就直接使用
     * 
     * @param function
     */
    public String getDbFunction(IFunction func);

    /**
     * 清除函数计算的中间结果，group by时使用
     */
    void clear();
}
