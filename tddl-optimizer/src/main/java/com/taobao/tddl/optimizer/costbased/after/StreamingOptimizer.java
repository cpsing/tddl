package com.taobao.tddl.optimizer.costbased.after;

import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.model.ExtraCmd;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.optimizer.core.expression.IBindVal;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;
import com.taobao.tddl.optimizer.core.plan.query.IMerge;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;

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
                                      Map<String, Object> extraCmd) {

        if (dne instanceof IQueryTree) {
            this.findMergeAndOptimizeStreaming(dne, extraCmd);
        }

        return dne;
    }

    void findMergeAndOptimizeStreaming(IDataNodeExecutor dne, Map<String, Object> extraCmd) {
        if (dne instanceof IMerge) {
            Comparable from = ((IMerge) dne).getLimitFrom();
            Comparable to = ((IMerge) dne).getLimitTo();
            boolean useStreaming = false;
            if ((from instanceof IBindVal) || (to instanceof IBindVal)) {
                useStreaming = true;
            }

            if (from instanceof Long && isNeedStreaming((Long) from, extraCmd)
                || (to instanceof Long && isNeedStreaming((Long) to, extraCmd))) {
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
            this.findMergeAndOptimizeStreaming(((IJoin) dne).getLeftNode(), extraCmd);
            this.findMergeAndOptimizeStreaming(((IJoin) dne).getRightNode(), extraCmd);
        }

        if (dne instanceof IQuery && ((IQuery) dne).getSubQuery() != null) {
            this.findMergeAndOptimizeStreaming(((IQuery) dne).getSubQuery(), extraCmd);
        }

    }

    private static boolean isNeedStreaming(Long limit, Map<String, Object> extraCmd) {
        return limit > GeneralUtil.getExtraCmdLong(extraCmd, ExtraCmd.STREAMI_THRESHOLD, 100);
    }
}
