package com.taobao.tddl.optimizer.core.expression;

import java.util.List;

import com.taobao.tddl.optimizer.core.function.IExtraFunction;

/**
 * 代表一个函数列，比如max(id)
 * 
 * @author jianghang 2013-11-8 下午1:29:03
 * @since 5.1.0
 */
public interface IFunction<RT extends IFunction> extends ISelectable<RT> {

    public static interface BuiltInFunction {

        final static String ADD           = "ADD";
        final static String SUB           = "SUB";
        final static String MUL           = "MYLTIPLY";
        final static String DIV           = "Division";
        final static String AVG           = "AVG";
        final static String SUM           = "SUM";
        final static String COUNT         = "COUNT";
        final static String MAX           = "MAX";
        final static String MIN           = "MIN";
        final static String INTERVAL      = "INTERVAL_PRIMARY";
        final static String GET_FORMAT    = "GET_FORMAT";
        final static String TIMESTAMPADD  = "TIMESTAMPADD";
        final static String TIMESTAMPDIFF = "TIMESTAMPDIFF";
        final static String CAST          = "CAST";
    }

    public enum FunctionType {
        /** 函数的操作面向一系列的值，并返回一个单一的值，可以理解为聚合函数 */
        Aggregate,
        /** 函数的操作面向某个单一的值，并返回基于输入值的一个单一的值，可以理解为转换函数 */
        Scalar;
    }

    public FunctionType getFunctionType();

    public String getFunctionName();

    public IFunction setFunctionName(String funcName);

    // TODO 不知道这些参数怎么用，做到后面查询树构造再来看看
    public List getMapArgs();

    public List getReduceArgs();

    public List getArgs();

    public RT setArgs(List objs);

    public int getExtraFuncArgSize();

    public IExtraFunction getExtraFunction();

    public boolean isNeedDistinctArg();

    public RT setNeedDistinctArg(boolean b);

    /**
     * 清除函数计算的中间结果，group by时使用
     */
    public void clear();

}
