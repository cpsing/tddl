package com.taobao.tddl.optimizer.costbased.after;

import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.model.ExtraCmd;
import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;
import com.taobao.tddl.optimizer.core.plan.query.IMerge;
import com.taobao.tddl.optimizer.core.plan.query.IParallelizableQueryTree.QUERY_CONCURRENCY;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;
import com.taobao.tddl.optimizer.costbased.before.QueryPlanOptimizer;

/**
 * 会修改一个状态标记。
 * 
 * @author Whisper
 */
public class MergeConcurrentOptimizer implements QueryPlanOptimizer {

    public MergeConcurrentOptimizer(){
    }

    /**
     * 如果设置了OptimizerExtraCmd.MergeConcurrent 并且值为True，则将所有的Merge变为并行
     */
    @Override
    public IDataNodeExecutor optimize(IDataNodeExecutor dne, Map<Integer, ParameterContext> parameterSettings,
                                      Map<String, Comparable> extraCmd) {
        boolean mergeConcurrentBool = false;
        if (!(extraCmd == null) && !extraCmd.isEmpty()) {
            Comparable mergeConcurrent = extraCmd.get(ExtraCmd.OptimizerExtraCmd.MergeConcurrent);
            if (mergeConcurrent != null) {
                if (mergeConcurrent instanceof String) {
                    mergeConcurrentBool = Boolean.valueOf(TStringUtil.trim(String.valueOf(mergeConcurrent)));
                } else if (mergeConcurrent instanceof Boolean) {
                    mergeConcurrentBool = (Boolean) mergeConcurrent;
                }
            }
        }

        if (dne instanceof IQueryTree && mergeConcurrentBool) {
            this.findMergeAndSetConcurrent(dne, mergeConcurrentBool);
        }

        return dne;
    }

    private void findMergeAndSetConcurrent(IDataNodeExecutor dne, boolean mergeConcurrentBool) {
        if (dne instanceof IMerge) {
            if (mergeConcurrentBool) {
                ((IMerge) dne).setQueryConcurrency(QUERY_CONCURRENCY.CONCURRENT);
            } else {
                ((IMerge) dne).setQueryConcurrency(QUERY_CONCURRENCY.SEQUENTIAL);
            }

            for (IDataNodeExecutor child : ((IMerge) dne).getSubNode()) {
                this.findMergeAndSetConcurrent(child, mergeConcurrentBool);
            }
        }

        if (dne instanceof IJoin) {
            this.findMergeAndSetConcurrent(((IJoin) dne).getLeftNode(), mergeConcurrentBool);
            this.findMergeAndSetConcurrent(((IJoin) dne).getRightNode(), mergeConcurrentBool);
        }

        if (dne instanceof IQuery && ((IQuery) dne).getSubQuery() != null) {
            this.findMergeAndSetConcurrent(((IQuery) dne).getSubQuery(), mergeConcurrentBool);

        }

    }
}
