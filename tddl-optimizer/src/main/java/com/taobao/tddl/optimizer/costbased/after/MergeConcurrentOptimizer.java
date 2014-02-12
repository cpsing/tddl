package com.taobao.tddl.optimizer.costbased.after;

import java.util.Map;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.model.ExtraCmd;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;
import com.taobao.tddl.optimizer.core.plan.query.IMerge;
import com.taobao.tddl.optimizer.core.plan.query.IParallelizableQueryTree.QUERY_CONCURRENCY;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;

/**
 * 会修改一个状态标记。
 * 
 * @author Whisper
 */
public class MergeConcurrentOptimizer implements QueryPlanOptimizer {

    public MergeConcurrentOptimizer(){
    }

    /**
     * 如果设置了MergeConcurrent 并且值为True，则将所有的Merge变为并行
     */
    @Override
    public IDataNodeExecutor optimize(IDataNodeExecutor dne, Map<Integer, ParameterContext> parameterSettings,
                                      Map<String, Object> extraCmd) {
        this.findMergeAndSetConcurrent(dne, extraCmd);

        return dne;
    }

    private void findMergeAndSetConcurrent(IDataNodeExecutor dne, Map<String, Object> extraCmd) {
        if (dne instanceof IMerge) {
            if (isMergeConcurrent(extraCmd, (IMerge) dne)) {
                ((IMerge) dne).setQueryConcurrency(QUERY_CONCURRENCY.CONCURRENT);
            } else {
                ((IMerge) dne).setQueryConcurrency(QUERY_CONCURRENCY.SEQUENTIAL);
            }

            for (IDataNodeExecutor child : ((IMerge) dne).getSubNode()) {
                this.findMergeAndSetConcurrent(child, extraCmd);
            }
        }

        if (dne instanceof IJoin) {
            this.findMergeAndSetConcurrent(((IJoin) dne).getLeftNode(), extraCmd);
            this.findMergeAndSetConcurrent(((IJoin) dne).getRightNode(), extraCmd);
        }

        if (dne instanceof IQuery && ((IQuery) dne).getSubQuery() != null) {
            this.findMergeAndSetConcurrent(((IQuery) dne).getSubQuery(), extraCmd);

        }
    }

    private static boolean isMergeConcurrent(Map<String, Object> extraCmd, IMerge query) {
        String value = ObjectUtils.toString(GeneralUtil.getExtraCmdString(extraCmd, ExtraCmd.MERGE_CONCURRENT));
        if (StringUtils.isEmpty(value)) {
            if (query.getSql() != null) {
                // 如果存在sql，那说明是hint直接路由
                return true;
            }

            if ((query.getLimitFrom() != null || query.getLimitTo() != null)) {
                if (query.getOrderBys() == null || query.getOrderBys().isEmpty()) {
                    // 存在limit，但不存在order by时不允许走并行
                    return false;
                }
            } else if ((query.getOrderBys() == null || query.getOrderBys().isEmpty())
                       && (query.getGroupBys() == null || query.getGroupBys().isEmpty())
                       && query.getHavingFilter() == null) {
                if (isNoFilter(query)) {
                    // 没有其他的order by / group by / having /
                    // where等条件时，就是个简单的select *
                    // from xxx，暂时也不做并行
                    return false;
                }
            }

            return true;
        } else {
            return BooleanUtils.toBoolean(value);
        }
    }

    private static boolean isNoFilter(IDataNodeExecutor dne) {
        if (!(dne instanceof IQueryTree)) {
            return true;
        }

        if (dne instanceof IMerge) {
            for (IDataNodeExecutor child : ((IMerge) dne).getSubNode()) {
                return isNoFilter(child);
            }
        }

        if (dne instanceof IJoin) {
            return isNoFilter(((IJoin) dne).getLeftNode()) && isNoFilter(((IJoin) dne).getRightNode());
        }

        if (dne instanceof IQuery) {
            if (((IQuery) dne).getSubQuery() != null) {
                return isNoFilter(((IQuery) dne).getSubQuery());
            } else {
                return ((IQuery) dne).getKeyFilter() == null && ((IQuery) dne).getValueFilter() == null;
            }

        }

        return true;
    }
}
