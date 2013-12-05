package com.taobao.tddl.repo.mysql.sqlconvertor.functions;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.repo.mysql.sqlconvertor.MysqlPlanVisitorImpl;

public interface FunctionStringConstructor {
//
//    public void constructColumnNameForFunction(OneQuery oneQuery, boolean bindVal, AtomicInteger bindValSequence,
//                                               Map<Integer, ParameterContext> paramMap, boolean isRetColumn,
//                                               StringBuilder sb, IFunction func);
    
    public String constructColumnNameForFunction(IDataNodeExecutor query, boolean bindVal, AtomicInteger bindValSequence,
                                               Map<Integer, ParameterContext> paramMap,
                                                IFunction func,MysqlPlanVisitorImpl parentVisitor);
}
