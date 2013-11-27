package com.taobao.tddl.optimizer.costbased.after;

import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.optimizer.core.expression.IBindVal;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;
import com.taobao.tddl.optimizer.core.plan.query.IMerge;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;
import com.taobao.tddl.optimizer.costbased.before.QueryPlanOptimizer;

/**
 * merge带limit的用streaming模式
 * 
 * @author Whisper
 */
public class StreamingOptimizer implements QueryPlanOptimizer {

    public StreamingOptimizer(){
    }

    @Override
    public IDataNodeExecutor optimize(IDataNodeExecutor dne, Map<Integer, ParameterContext> parameterSettings,
                                      Map<String, Comparable> extraCmd) {

        if (dne instanceof IQueryTree) {
            this.findMergeAndOptimizeStreaming(dne);
        }

        return dne;
    }

    void findMergeAndOptimizeStreaming(IDataNodeExecutor dne) {
        if (dne instanceof IMerge) {
            Comparable from = ((IMerge) dne).getLimitFrom();
            Comparable to = ((IMerge) dne).getLimitTo();
            boolean useStreaming = false;
            if ((from instanceof IBindVal) || (to instanceof IBindVal)) {
                useStreaming = true;
            }

            if ((from instanceof Long && ((Long) from) != 0) || (to instanceof Long && ((Long) to) != 0)) {
                useStreaming = true;
            }

            if (useStreaming) {
                for (IDataNodeExecutor child : ((IMerge) dne).getSubNode()) {
                    if (child instanceof IQueryTree) {
                        ((IQueryTree) child).setStreaming(true);
                    }
                }
            }

        }
        if (dne instanceof IJoin) {
            this.findMergeAndOptimizeStreaming(((IJoin) dne).getLeftNode());
            this.findMergeAndOptimizeStreaming(((IJoin) dne).getRightNode());
        }

        if (dne instanceof IQuery && ((IQuery) dne).getSubQuery() != null) {
            this.findMergeAndOptimizeStreaming(((IQuery) dne).getSubQuery());
        }

    }
}
