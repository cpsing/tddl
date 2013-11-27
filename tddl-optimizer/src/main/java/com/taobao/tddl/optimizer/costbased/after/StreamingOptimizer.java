package com.taobao.tddl.optimizer.costbased.after;

import java.util.Map;

import com.taobao.ustore.common.inner.bean.IBindVal;
import com.taobao.ustore.common.inner.bean.IDataNodeExecutor;
import com.taobao.ustore.common.inner.bean.IJoin;
import com.taobao.ustore.common.inner.bean.IMerge;
import com.taobao.ustore.common.inner.bean.IQuery;
import com.taobao.ustore.common.inner.bean.IQueryCommon;
import com.taobao.ustore.common.inner.bean.ParameterContext;
import com.taobao.ustore.optimizer.QueryPlanOptimizer;

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

        if (dne instanceof IQueryCommon) {
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
                    if (child instanceof IQueryCommon) {
                        ((IQueryCommon) child).setStreaming(true);
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
