package com.taobao.tddl.repo.mysql.sqlconvertor.functions;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.repo.mysql.sqlconvertor.MysqlPlanVisitorImpl;

public class Cast implements FunctionStringConstructor {

    @Override
    public String constructColumnNameForFunction(IDataNodeExecutor query, boolean bindVal,
                                                 AtomicInteger bindValSequence,
                                                 Map<Integer, ParameterContext> paramMap, IFunction func,
                                                 MysqlPlanVisitorImpl parentVisitor) {
        StringBuilder sb = new StringBuilder();

        sb.append(IFunction.BuiltInFunction.CAST).append("(");
        sb.append(parentVisitor.getNewVisitor(func.getArgs().get(0)).getString());
        sb.append(" as ");
        // typename
        sb.append(func.getArgs().get(1));

        // typeinfo
        if (func.getArgs().size() > 2) {
            sb.append("(");
            // typeinfo1
            sb.append(parentVisitor.getNewVisitor(func.getArgs().get(2)).getString());

            // typeinfo2
            if (func.getArgs().size() > 3) {
                sb.append(",");
                sb.append(parentVisitor.getNewVisitor(func.getArgs().get(3)).getString());
            }
            sb.append(")");
        }
        sb.append(")");

        return sb.toString();

    }

}
