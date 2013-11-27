package com.taobao.tddl.optimizer.costbased.after;

import java.util.Map;

import com.taobao.ustore.common.inner.bean.ExtraCmd;
import com.taobao.ustore.common.inner.bean.IDataNodeExecutor;
import com.taobao.ustore.common.inner.bean.IJoin;
import com.taobao.ustore.common.inner.bean.IMerge;
import com.taobao.ustore.common.inner.bean.IQuery;
import com.taobao.ustore.common.inner.bean.IQueryCommon;
import com.taobao.ustore.common.inner.bean.ParameterContext;
import com.taobao.ustore.common.inner.bean.IParallelizableQueryCommon.QUERY_CONCURRENCY;
import com.taobao.ustore.optimizer.QueryPlanOptimizer;
import com.taobao.ustore.optimizer.util.StringUtil;

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
                    mergeConcurrentBool = Boolean.valueOf(StringUtil.trim(String.valueOf(mergeConcurrent)));
                } else if (mergeConcurrent instanceof Boolean) {
                    mergeConcurrentBool = (Boolean) mergeConcurrent;
                }
            }
        }

        if (dne instanceof IQueryCommon && mergeConcurrentBool) {
            this.findMergeAndSetConcurrent(dne, mergeConcurrentBool);
        }

        return dne;
    }

    void findMergeAndSetConcurrent(IDataNodeExecutor dne, boolean mergeConcurrentBool) {
        if (dne instanceof IMerge) {
            if (mergeConcurrentBool) ((IMerge) dne).setQueryConcurrency(QUERY_CONCURRENCY.CONCURRENT);
            else ((IMerge) dne).setQueryConcurrency(QUERY_CONCURRENCY.SEQUENTIAL);
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
