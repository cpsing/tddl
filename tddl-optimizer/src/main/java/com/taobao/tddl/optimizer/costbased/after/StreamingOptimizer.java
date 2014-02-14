package com.taobao.tddl.optimizer.costbased.after;

import java.util.Map;

import com.taobao.tddl.common.TddlConstants;
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
 * streaming模式处理
 * 
 * <pre>
 * 几种情况需要使用streaming
 * 1. merge节点中带limit，需要设置子节点为streaming模式
 * 2. join节点+不可下推，需要设置左右节点为streaming模式
 * 3. query节点+不可下推，需要设置子节点为streaming模式
 * </pre>
 * 
 * @author Whisper
 * @author <a href="jianghang.loujh@taobao.com">jianghang</a>
 */
public class StreamingOptimizer implements QueryPlanOptimizer {

    public StreamingOptimizer(){
    }

    @Override
    public IDataNodeExecutor optimize(IDataNodeExecutor dne, Map<Integer, ParameterContext> parameterSettings,
                                      Map<String, Object> extraCmd) {

        if (dne instanceof IQueryTree) {
            this.findQueryOptimizeStreaming(dne, false, extraCmd);
        }

        return dne;
    }

    private void findQueryOptimizeStreaming(IDataNodeExecutor dne, boolean isNeedStreaming, Map<String, Object> extraCmd) {
        if (!(dne instanceof IQueryTree)) {
            return;
        }

        boolean streaming = isNeedStreaming | isNeedStreaming((IQueryTree) dne, extraCmd);
        if (dne instanceof IMerge) {
            for (IDataNodeExecutor child : ((IMerge) dne).getSubNode()) {
                if (streaming) {
                    child.setStreaming(true);
                }
                // 让子节点递归进行计算
                this.findQueryOptimizeStreaming(child, streaming, extraCmd);
            }
        } else if (dne instanceof IJoin) {
            if (streaming) {
                // 如果父节点要求其为straming，让左右节点为streaming
                // 如果是join节点存在limit，让左右节点为streaming
                // 如果整个join节点可下推，执行节点只会在join上，不会到left/right节点，此时streaming无效，所以不用做这判断
                ((IJoin) dne).getLeftNode().setStreaming(true);
                ((IJoin) dne).getRightNode().setStreaming(true);
            }

            this.findQueryOptimizeStreaming(((IJoin) dne).getLeftNode(), streaming, extraCmd);
            this.findQueryOptimizeStreaming(((IJoin) dne).getRightNode(), streaming, extraCmd);
        } else if (dne instanceof IQuery) {
            if (streaming) {
                // 如果父节点要求其为straming，让其子节点为streaming
                // 如果是query节点存在limit，让其子节点为streaming
                ((IQuery) dne).getSubQuery().setStreaming(true);
            }

            if (((IQuery) dne).getSubQuery() != null) {
                this.findQueryOptimizeStreaming(((IQuery) dne).getSubQuery(), streaming, extraCmd);
            }
        }

    }

    private static boolean isNeedStreaming(IQueryTree query, Map<String, Object> extraCmd) {
        Comparable from = query.getLimitFrom();
        Comparable to = query.getLimitTo();
        boolean useStreaming = false;
        if ((from instanceof IBindVal) || (to instanceof IBindVal)) {
            useStreaming = true;
        }

        if (from instanceof Long && isNeedStreaming((Long) from, extraCmd)
            || (to instanceof Long && isNeedStreaming((Long) to, extraCmd))) {
            useStreaming = true;
        }
        return useStreaming;
    }

    private static boolean isNeedStreaming(Long limit, Map<String, Object> extraCmd) {
        return limit > GeneralUtil.getExtraCmdLong(extraCmd,
            ExtraCmd.STREAMI_THRESHOLD,
            TddlConstants.DEFAULT_STREAM_THRESOLD);
    }
}
