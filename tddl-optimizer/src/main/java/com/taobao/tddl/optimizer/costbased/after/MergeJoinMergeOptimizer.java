package com.taobao.tddl.optimizer.costbased.after;

import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.model.ExtraCmd;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;
import com.taobao.tddl.optimizer.core.plan.query.IJoin.JoinStrategy;
import com.taobao.tddl.optimizer.core.plan.query.IMerge;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;

/**
 * 如果设置了MergeConcurrent 并且值为True，则将所有的Merge变为并行
 * 
 * @since 5.0.0
 */
public class MergeJoinMergeOptimizer implements QueryPlanOptimizer {

    /**
     * 如果设置了MergeConcurrent 并且值为True，则将所有的Merge变为并行
     */
    @Override
    public IDataNodeExecutor optimize(IDataNodeExecutor dne, Map<Integer, ParameterContext> parameterSettings,
                                      Map<String, Object> extraCmd) {
        if (isMergeExpand(extraCmd)) {
            return this.findEveryJoin(dne, true, true);
        } else {
            return dne;
        }
    }

    private IDataNodeExecutor findEveryJoin(IDataNodeExecutor dne, boolean isExpandLeft, boolean isExpandRight) {
        if (dne instanceof IMerge) {
            List<IDataNodeExecutor> subs = ((IMerge) dne).getSubNode();
            for (int i = 0; i < subs.size(); i++) {
                subs.set(i, this.findEveryJoin(subs.get(i), isExpandLeft, isExpandRight));
            }

            ((IMerge) dne).setSubNode(subs);
            return dne;
        } else if (dne instanceof IQuery) {
            return dne;
        } else if (dne instanceof IJoin) {
            ((IJoin) dne).setLeftNode((IQueryTree) this.findEveryJoin(((IJoin) dne).getLeftNode(),
                isExpandLeft,
                isExpandRight));
            ((IJoin) dne).setRightNode((IQueryTree) this.findEveryJoin(((IJoin) dne).getRightNode(),
                isExpandLeft,
                isExpandRight));
            return this.processJoin((IJoin) dne, isExpandLeft, isExpandRight);
        }

        return dne;
    }

    private IQueryTree processJoin(IJoin j, boolean isExpandLeft, boolean isExpandRight) {
        // 如果一个节点包含limit，group by，order by等条件，则不能展开
        if (!canExpand(j)) {
            // join节点可能自己存在limit
            isExpandLeft = false;
            isExpandRight = false;
        } else if (!canExpand(j.getLeftNode())) {
            isExpandLeft = false;
        } else if (!canExpand(j.getRightNode())) {
            isExpandRight = false;
        }

        if (isExpandLeft && isExpandRight) {
            return this.cartesianProduct(j);
        } else if (isExpandLeft) {
            return this.expandLeft(j);
        } else if (isExpandRight) {
            return this.expandRight(j);
        }

        return this.mergeJoinMerge(j);
    }

    private IJoin mergeJoinMerge(IJoin j) {
        // j.setJoinType(JoinType.SORT_MERGE_JOIN);
        return j;
    }

    private boolean canExpand(IQueryTree query) {
        // 如果一个节点包含limit，group by，order by等条件
        if (query.getLimitFrom() != null || query.getLimitTo() != null || query.getGroupBys() != null
            || query.getOrderBys() != null) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * 将左边的merge展开，依次和右边做join
     */
    public IQueryTree expandLeft(IJoin j) {
        if (!(j.getLeftNode() instanceof IMerge)) {
            j.setJoinStrategy(JoinStrategy.SORT_MERGE_JOIN);
            return j;
        }

        IMerge left = (IMerge) j.getLeftNode();
        IMerge newMerge = ASTNodeFactory.getInstance().createMerge();

        for (IDataNodeExecutor leftChild : left.getSubNode()) {
            IJoin newJoin = (IJoin) j.copy();
            newJoin.setJoinStrategy(JoinStrategy.SORT_MERGE_JOIN);
            newJoin.setLeftNode((IQueryTree) leftChild);
            newJoin.setRightNode(j.getRightNode());
            newJoin.executeOn(j.getDataNode());
            newMerge.addSubNode(newJoin);
        }

        newMerge.setAlias(j.getAlias());
        newMerge.setColumns(j.getColumns());
        newMerge.setConsistent(j.getConsistent());
        newMerge.setGroupBys(j.getGroupBys());
        newMerge.setLimitFrom(j.getLimitFrom());
        newMerge.setLimitTo(j.getLimitTo());
        newMerge.setOrderBys(j.getOrderBys());
        newMerge.setQueryConcurrency(j.getQueryConcurrency());
        newMerge.setValueFilter(j.getValueFilter());
        newMerge.executeOn(j.getDataNode());
        return newMerge;
    }

    /**
     * 将右边的merge展开，依次和左边做join
     * 
     * @param j
     * @return
     */
    public IQueryTree expandRight(IJoin j) {
        // merge的大小大于1时，才会展开
        if (!(j.getRightNode() instanceof IMerge && ((IMerge) j.getRightNode()).getSubNode().size() > 1)) {
            return j;
        }

        IMerge right = (IMerge) j.getRightNode();
        IMerge newMerge = ASTNodeFactory.getInstance().createMerge();
        for (IDataNodeExecutor rightChild : right.getSubNode()) {
            IJoin newJoin = (IJoin) j.copy();
            newJoin.setLeftNode(j.getLeftNode());
            ((IQueryTree) rightChild).setAlias(right.getAlias());
            newJoin.setRightNode((IQueryTree) rightChild);
            newJoin.executeOn(j.getDataNode());
            newMerge.addSubNode(newJoin);
        }

        newMerge.setAlias(j.getAlias());
        newMerge.setColumns(j.getColumns());
        newMerge.setConsistent(j.getConsistent());
        newMerge.setGroupBys(j.getGroupBys());
        newMerge.setLimitFrom(j.getLimitFrom());
        newMerge.setLimitTo(j.getLimitTo());
        newMerge.setOrderBys(j.getOrderBys());
        newMerge.setQueryConcurrency(j.getQueryConcurrency());
        newMerge.setValueFilter(j.getValueFilter());
        newMerge.executeOn(j.getDataNode());
        return newMerge;
    }

    /**
     * 左右都展开做笛卡尔积
     * 
     * @param j
     * @return
     */
    public IQueryTree cartesianProduct(IJoin j) {
        if (j.getLeftNode() instanceof IMerge && !(j.getRightNode() instanceof IMerge)) {
            return this.expandLeft(j);
        }

        if (!(j.getLeftNode() instanceof IMerge) && (j.getRightNode() instanceof IMerge)) {
            return this.expandRight(j);
        }

        if (!(j.getLeftNode() instanceof IMerge) && !(j.getRightNode() instanceof IMerge)) {
            return j;
        }

        IMerge leftMerge = (IMerge) j.getLeftNode();
        IMerge rightMerge = (IMerge) j.getRightNode();
        IMerge newMerge = ASTNodeFactory.getInstance().createMerge();

        for (IDataNodeExecutor leftChild : leftMerge.getSubNode()) {
            for (IDataNodeExecutor rightChild : rightMerge.getSubNode()) {
                IJoin newJoin = (IJoin) j.copy();
                newJoin.setLeftNode((IQueryTree) leftChild);
                newJoin.setRightNode((IQueryTree) rightChild);
                newJoin.executeOn(leftChild.getDataNode());
                newMerge.addSubNode(newJoin);
            }
        }
        newMerge.setAlias(j.getAlias());
        newMerge.setColumns(j.getColumns());
        newMerge.setConsistent(j.getConsistent());
        newMerge.setGroupBys(j.getGroupBys());
        newMerge.setLimitFrom(j.getLimitFrom());
        newMerge.setLimitTo(j.getLimitTo());
        newMerge.setOrderBys(j.getOrderBys());
        newMerge.setQueryConcurrency(j.getQueryConcurrency());
        newMerge.setValueFilter(j.getValueFilter());
        newMerge.executeOn(j.getDataNode());
        return newMerge;
    }

    private static boolean isMergeExpand(Map<String, Object> extraCmd) {
        return GeneralUtil.getExtraCmdBoolean(extraCmd, ExtraCmd.MERGE_EXPAND, false);
    }
}
