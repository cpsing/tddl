package com.taobao.tddl.optimizer.costbased.after;

import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.model.ExtraCmd;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;
import com.taobao.tddl.optimizer.core.plan.query.IJoin.JoinType;
import com.taobao.tddl.optimizer.core.plan.query.IMerge;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;

/**
 * 如果设置了OptimizerExtraCmd.MergeConcurrent 并且值为True，则将所有的Merge变为并行
 * 
 * @since 5.1.0
 */
public class MergeJoinMergeOptimizer implements QueryPlanOptimizer {

    /**
     * 如果设置了OptimizerExtraCmd.MergeConcurrent 并且值为True，则将所有的Merge变为并行
     */
    @Override
    public IDataNodeExecutor optimize(IDataNodeExecutor dne, Map<Integer, ParameterContext> parameterSettings,
                                      Map<String, Comparable> extraCmd) {

        boolean isExpandLeft = false;
        boolean isExpandRight = false;
        if (extraCmd != null && !extraCmd.isEmpty()) {
            if (extraCmd.get(ExtraCmd.OptimizerExtraCmd.ExpandLeft) != null) {
                isExpandLeft = (Boolean) extraCmd.get(ExtraCmd.OptimizerExtraCmd.ExpandLeft);
            }

            if (extraCmd.get(ExtraCmd.OptimizerExtraCmd.ExpandRight) != null) {
                isExpandRight = (Boolean) extraCmd.get(ExtraCmd.OptimizerExtraCmd.ExpandRight);
            }
        }

        if (!isExpandLeft && !isExpandRight) {
            return dne;
        }

        return this.findEveryJoin(dne, isExpandLeft, isExpandRight);
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
        // 如果一个节点包含limit，group by，order by等条件，
        // 则不能展开
        if (j.getLeftNode().getLimitFrom() != null || j.getLeftNode().getLimitTo() != null
            || (j.getLeftNode().getGroupBys() != null && !j.getLeftNode().getGroupBys().isEmpty())
            || (j.getLeftNode().getOrderBys() != null && !j.getLeftNode().getGroupBys().isEmpty())) {
            isExpandLeft = false;
        }

        if (j.getRightNode().getLimitFrom() != null || j.getRightNode().getLimitTo() != null
            || (j.getRightNode().getGroupBys() != null && !j.getRightNode().getGroupBys().isEmpty())
            || (j.getRightNode().getOrderBys() != null && !j.getRightNode().getOrderBys().isEmpty())) {
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

    /**
     * 将左边的merge展开，依次和右边做join
     */
    public IQueryTree expandLeft(IJoin j) {
        if (!(j.getLeftNode() instanceof IMerge)) {
            j.setJoinType(JoinType.SORT_MERGE_JOIN);
            return j;
        }

        IMerge left = (IMerge) j.getLeftNode();
        IMerge newMerge = ASTNodeFactory.getInstance().createMerge();

        for (IDataNodeExecutor leftChild : left.getSubNode()) {
            IJoin newJoin = (IJoin) j.copy();
            newJoin.setJoinType(JoinType.SORT_MERGE_JOIN);
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
        newMerge.setValueFilter(j.getResultSetFilter());
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
        newMerge.setValueFilter(j.getResultSetFilter());
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
        newMerge.setValueFilter(j.getResultSetFilter());
        newMerge.executeOn(j.getDataNode());
        return newMerge;
    }

}
