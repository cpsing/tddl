package com.taobao.tddl.optimizer.costbased.after;

import java.math.BigDecimal;
import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;
import com.taobao.tddl.optimizer.core.plan.query.IMerge;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;

/**
 * 遍历所有的节点，如果有merge的情况下，记录下merge的limit from to。 会修改所有merge节点下面的limit from to
 * 
 * @author Whisper
 */
public class LimitOptimizer implements QueryPlanOptimizer {

    public LimitOptimizer(){
    }

    /**
     * 如果设置了MergeConcurrent 并且值为True，则将所有的Merge变为并行
     */
    @Override
    public IDataNodeExecutor optimize(IDataNodeExecutor dne, Map<Integer, ParameterContext> parameterSettings,
                                      Map<String, Object> extraCmd) {

        if (dne instanceof IQueryTree) {
            this.findMergeAndOptimizerLimit(dne);
        }

        return dne;
    }

    void findMergeAndOptimizerLimit(IDataNodeExecutor dne) {

        if (dne instanceof IQueryTree) {
            IQueryTree q = (IQueryTree) dne;

            if (q.getLimitFrom() instanceof Integer) {
                q.setLimitFrom(new Long((Integer) q.getLimitFrom()));
            }

            if (q.getLimitFrom() instanceof BigDecimal) {
                q.setLimitFrom(new Long(((BigDecimal) q.getLimitFrom()).longValue()));
            }

            if (q.getLimitTo() instanceof Integer) {
                q.setLimitTo(new Long((Integer) q.getLimitTo()));
            }
            if (q.getLimitTo() instanceof BigDecimal) {
                q.setLimitTo(new Long(((BigDecimal) q.getLimitTo()).longValue()));
            }
        }

        if (dne instanceof IMerge) {
            Comparable from = ((IMerge) dne).getLimitFrom();
            Comparable to = ((IMerge) dne).getLimitTo();

            if (from instanceof Long && to instanceof Long) {
                if ((from != null && (Long) from != -1) || (to != null && (Long) to != -1)) {
                    for (IDataNodeExecutor child : ((IMerge) dne).getSubNode()) {
                        if (child instanceof IQueryTree) {
                            ((IQueryTree) child).setLimitFrom(0L);
                            ((IQueryTree) child).setLimitTo((Long) from + (Long) to);
                        }

                    }
                }
            }
            for (IDataNodeExecutor child : ((IMerge) dne).getSubNode())
                this.findMergeAndOptimizerLimit(child);
        }

        if (dne instanceof IJoin) {
            this.findMergeAndOptimizerLimit(((IJoin) dne).getLeftNode());
            this.findMergeAndOptimizerLimit(((IJoin) dne).getRightNode());
        }

        if (dne instanceof IQuery && ((IQuery) dne).getSubQuery() != null) {
            this.findMergeAndOptimizerLimit(((IQuery) dne).getSubQuery());

        }

    }
}
