package com.taobao.tddl.optimizer.costbased.chooser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.query.JoinNode;
import com.taobao.tddl.optimizer.core.ast.query.QueryNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.utils.PermutationGenerator;

/**
 * 对于inner join的所有节点生成一个全排列
 * 
 * @author Dreamond
 */
public final class JoinPermutationGenerator {

    private PermutationGenerator                               pg                   = null;
    private List<IOrderBy>                                     orderBys;
    private IFilter                                            valueFilter;
    private Comparable                                         limitFrom;
    private Comparable                                         limitTo;
    private List<IOrderBy>                                     groupBys;

    /**
     * A join B on A.id = B.id 转变为 {A.id-> {B.id->BF[A.id=B.id]}, B.id->
     * {A.id->BF[A.id=B.id]} }
     * 
     * <pre>
     * 第一层key: one column
     * 第二层key：a pair of Key col
     * 第三层value : the whole join on columns pair
     * </pre>
     */
    private Map<ISelectable, Map<ISelectable, IBooleanFilter>> joinColumnsAndFilter = new HashMap();
    private List<QueryTreeNode>                                queryNodes           = new ArrayList();
    private List<ISelectable>                                  columns;
    private IFilter                                            allWhereFilter;

    public JoinPermutationGenerator(QueryTreeNode qtn){
        this.orderBys = qtn.getOrderBys();
        this.groupBys = qtn.getGroupBys();
        this.columns = qtn.getColumnsSelected();
        this.valueFilter = qtn.getResultFilter();
        this.limitFrom = qtn.getLimitFrom();
        this.limitTo = qtn.getLimitTo();
        this.allWhereFilter = qtn.getAllWhereFilter();
        this.getQueryNodesFromQueryTree(qtn);

        pg = new PermutationGenerator(this.queryNodes);
        visit(qtn);
    }

    /**
     * 从查询树中收集不会再被调整Join顺序的子树
     * 
     * <pre>
     * 包括： 
     * 1、非InnerJoin的Join节点。因为既然用户已经指定了left或者right，说明用户已经指定了outter的就为驱动表，所以无需再做额外的调整
     * 2、被标记为子查询的Join节点，子查询中的节点会单独去调整
     * 3、不在1中的Query节点
     * </pre>
     */
    private void getQueryNodesFromQueryTree(QueryTreeNode node) {
        if (node instanceof JoinNode) {
            if (!((JoinNode) node).isInnerJoin() || node.isSubQuery()
                || ((JoinNode) node).getOtherJoinOnFilter() != null) {
                this.queryNodes.add(node);
                return;
            }
        }

        if (node instanceof TableNode || node instanceof QueryNode) {
            this.queryNodes.add(node);
            return;
        }

        for (ASTNode child : node.getChildren()) {
            getQueryNodesFromQueryTree((QueryTreeNode) child);
        }

    }

    private void visit(QueryTreeNode node) {
        if (node instanceof JoinNode) {
            List<ISelectable> leftColumns = ((JoinNode) node).getLeftKeys();
            List<ISelectable> rightColumns = ((JoinNode) node).getRightKeys();
            assert (leftColumns.size() == rightColumns.size());
            for (IFilter filter : ((JoinNode) node).getJoinFilter()) {
                addJoinFilter((IBooleanFilter) filter);
            }
        }
        for (ASTNode child : node.getChildren()) {
            visit((QueryTreeNode) child);
        }
    }

    private void addJoinFilter(IBooleanFilter filter) {
        ISelectable left = (ISelectable) filter.getColumn();
        ISelectable right = (ISelectable) filter.getValue();
        if (!this.joinColumnsAndFilter.containsKey(left)) {
            this.joinColumnsAndFilter.put(left, new HashMap());
        }

        this.joinColumnsAndFilter.get(left).put(right, filter);
        // add by shenxun : 这里应该进行对调，map中应该维持左->右这个关系
        IBooleanFilter ibfnew = convertJoinOnColumns(filter);
        if (!this.joinColumnsAndFilter.containsKey(right)) {
            this.joinColumnsAndFilter.put(right, new HashMap());
        }

        this.joinColumnsAndFilter.get(right).put(left, ibfnew);
    }

    private IBooleanFilter convertJoinOnColumns(IBooleanFilter filter) {
        IBooleanFilter newbf = filter.copy();
        newbf.setColumn(filter.getValue());
        newbf.setValue((Comparable) filter.getColumn());
        return newbf;
    }

    public QueryTreeNode getNext() {
        while (pg.hasNext()) {
            List<QueryTreeNode> nodes = pg.next();
            for (int i = 0; i < nodes.size(); i++) {
                nodes.set(i, nodes.get(i).deepCopy());
            }

            QueryTreeNode newTree = getQueryTreeFromQueryNodes(nodes);
            if (newTree != null) {
                newTree.setOrderBys(orderBys);
                newTree.setResultFilter(this.valueFilter);
                newTree.setGroupBys(groupBys);
                newTree.select(this.columns);
                newTree.setLimitFrom(limitFrom);
                newTree.setLimitTo(limitTo);
                newTree.setAllWhereFilter(allWhereFilter);
                return newTree;
            }
        }

        return null;
    }

    /**
     * 构造一个join
     */
    private QueryTreeNode getQueryTreeFromQueryNodes(List<QueryTreeNode> nodes) {
        if (nodes.size() == 1) {
            return nodes.get(0);
        }

        JoinNode join = null;
        for (int i = 1; i < nodes.size(); i++) {
            List<IBooleanFilter> filterToJoinOn;

            if (join == null) {
                filterToJoinOn = canJoinAndThenReturnFilters(nodes.get(i - 1), nodes.get(i));
                if (filterToJoinOn == null || filterToJoinOn.isEmpty()) {
                    return null;
                }
                join = nodes.get(i - 1).join(nodes.get(i));
                join.setJoinFilter(filterToJoinOn);
            } else {
                filterToJoinOn = canJoinAndThenReturnFilters(join, nodes.get(i));
                if (filterToJoinOn == null) {
                    return null;
                }
                join = join.join(nodes.get(i));
                join.setJoinFilter(filterToJoinOn);

            }

            join.build();
        }
        return join;
    }

    /**
     * 找到left/right节点存在的join条件
     */
    private List<IBooleanFilter> canJoinAndThenReturnFilters(QueryTreeNode leftNode, QueryTreeNode rightNode) {
        List<IBooleanFilter> filters = new LinkedList();
        for (ISelectable leftColumn : leftNode.getColumnsSelectedForParent()) {
            if (!this.joinColumnsAndFilter.containsKey(leftColumn)) {
                continue;
            }

            Map<ISelectable, IBooleanFilter> rightColumnsAndFilter = this.joinColumnsAndFilter.get(leftColumn);
            List<ISelectable> rightColumns = rightNode.getColumnsSelectedForParent();
            for (ISelectable rightColumn : rightColumnsAndFilter.keySet()) {
                if (rightColumns.contains(rightColumn)) {
                    filters.add(rightColumnsAndFilter.get(rightColumn));
                }
            }
        }

        return filters;
    }
}
