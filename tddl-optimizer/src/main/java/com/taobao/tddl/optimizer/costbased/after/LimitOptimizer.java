package com.taobao.tddl.optimizer.costbased.after;

import java.util.Map;

import com.taobao.ustore.common.inner.bean.IDataNodeExecutor;
import com.taobao.ustore.common.inner.bean.IJoin;
import com.taobao.ustore.common.inner.bean.IMerge;
import com.taobao.ustore.common.inner.bean.IQuery;
import com.taobao.ustore.common.inner.bean.IQueryCommon;
import com.taobao.ustore.common.inner.bean.ParameterContext;
import com.taobao.ustore.optimizer.QueryPlanOptimizer;

/**
 * 遍历所有的节点，如果有merge的情况下，记录下merge的limit from to。 会修改所有merge节点下面的limit from to
 * 
 * @author Whisper
 */
public class LimitOptimizer implements QueryPlanOptimizer {

    public LimitOptimizer(){
    }

    /**
     * 如果设置了OptimizerExtraCmd.MergeConcurrent 并且值为True，则将所有的Merge变为并行
     */
    @Override
    public IDataNodeExecutor optimize(IDataNodeExecutor dne, Map<Integer, ParameterContext> parameterSettings,
                                      Map<String, Comparable> extraCmd) {

        if (dne instanceof IQueryCommon) {
            this.findMergeAndOptimizerLimit(dne);
        }

        return dne;
    }

    void findMergeAndOptimizerLimit(IDataNodeExecutor dne) {
        if (dne instanceof IMerge) {
            Comparable from = ((IMerge) dne).getLimitFrom();
            Comparable to = ((IMerge) dne).getLimitTo();

            if (from instanceof Long && to instanceof Long) {
                if ((from != null && (Long) from != -1) || (to != null && (Long) to != -1)) {
                    for (IDataNodeExecutor child : ((IMerge) dne).getSubNode()) {

                        if (child instanceof IQueryCommon) {
                            ((IQueryCommon) child).setLimitFrom(0L);
                            ((IQueryCommon) child).setLimitTo((Long) from + (Long) to);
                        }

                        this.findMergeAndOptimizerLimit(child);
                    }
                }
            }
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
