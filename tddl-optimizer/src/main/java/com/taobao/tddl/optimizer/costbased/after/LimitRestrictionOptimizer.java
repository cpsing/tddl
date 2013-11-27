package com.taobao.tddl.optimizer.costbased.after;

import java.util.Map;

import com.taobao.ustore.common.inner.bean.IDataNodeExecutor;
import com.taobao.ustore.common.inner.bean.IQueryCommon;
import com.taobao.ustore.common.inner.bean.ParameterContext;
import com.taobao.ustore.optimizer.QueryPlanOptimizer;

/**
 * limit 上限。如果有这个限制，那么limit m,n的时候会自动加上limit限制，如果她没有的话 会增加limit m,n限制
 * 
 * @author Whisper
 */
public class LimitRestrictionOptimizer implements QueryPlanOptimizer {

    public LimitRestrictionOptimizer(){
    }

    /**
     * 为limit添加上限，防止溢出
     */
    @Override
    public IDataNodeExecutor optimize(IDataNodeExecutor dne, Map<Integer, ParameterContext> parameterSettings,
                                      Map<String, Comparable> extraCmd) {

        if (dne instanceof IQueryCommon) {
            this.findMergeAndOptimizerLimit(dne);
        }

        Comparable to = ((IQueryCommon) dne).getLimitTo();

        if (to == null || to.equals(0L) || to.compareTo(1000L) > 0) {
            ((IQueryCommon) dne).setLimitTo(1000L);
        }
        return dne;
    }

    void findMergeAndOptimizerLimit(IDataNodeExecutor dne) {

    }
}
