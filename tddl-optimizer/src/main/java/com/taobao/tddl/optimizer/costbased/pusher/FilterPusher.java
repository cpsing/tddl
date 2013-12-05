package com.taobao.tddl.optimizer.costbased.pusher;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.query.JoinNode;
import com.taobao.tddl.optimizer.core.ast.query.QueryNode;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.exceptions.QueryException;
import com.taobao.tddl.optimizer.utils.FilterUtils;

/**
 * 将filter进行下推
 * 
 * <pre>
 * 如果条件中包含||条件则暂不优化，下推时会导致语义不正确
 * 
 * 几种场景：
 * 1. where条件尽可能提前到叶子节点，同时提取出joinFilter
 * 处理类型： JoinNode/QueryNode
 * 注意点：JoinNode如果是outter节点，则不能继续下推
 * 
 * 如： tabl1.join(table2).query("table1.id>5 && table2.id<10 && table1.name = table2.name")
 * 优化成: table1.query("table1.id>5").join(table2.query("table2.id<10").on("table1.name = table2.name")
 * 
 * 如: table1.join(table2).query("table1.id = table2.id")
 * 优化成：table1.join(table2).on("table1.id = table2.id")
 * 
 * 2. join中的非字段列条件，比如column = 1的常量关系，提前到叶子节点
 * 处理类型：JoinNode
 * 注意点：
 * 
 * 如： tabl1.join(table2).on("table1.id>5&&table2.id<10")
 * 优化成: table1.query("table1.id>5").join(table2.query("table2.id<10")) t但如果条件中包含
 * 
 * 3. join filter中的字段进行条件推导到左/右的叶子节点上，在第1和第2步优化中同时处理
 * 处理类型：JoinNode
 * 
 * 如: table.join(table2).on("table1.id = table2.id and table1.id>5 && table2.id<10")
 * 优化成：table1.query("table1.id>5 && table1.id<10").join(table2.query("table2.id>5 && table2.id<10"))
 */
public class FilterPusher {

    /**
     * 详细优化见类描述 {@linkplain FilterPusher}
     */
    public static QueryTreeNode optimize(QueryTreeNode qtn) throws QueryException {
        qtn = pushFilter(qtn, null);
        qtn = pushJoinOnFilter(qtn, null);
        qtn.build();
        return qtn;
    }

    private static QueryTreeNode pushFilter(QueryTreeNode qtn, List<IFilter> DNFNodeToPush) throws QueryException {
        // 如果是根节点，接收filter做为where条件,否则继续合并当前where条件，然后下推
        if (qtn.getChildren().isEmpty()) {
            IFilter node = FilterUtils.DNFToAndLogicTree(DNFNodeToPush);
            if (node != null) {
                qtn.query(FilterUtils.and(qtn.getWhereFilter(), (IFilter) node.copy()));
                qtn.build();
            }
            return qtn;
        }

        // 对于or连接的条件，就不能下推了
        IFilter filterInWhere = qtn.getWhereFilter();
        if (filterInWhere != null && FilterUtils.isDNF(filterInWhere)) {
            List<IFilter> DNFNode = FilterUtils.toDNFNode(filterInWhere);
            qtn.query((IFilter) null);// 清空where条件
            if (DNFNodeToPush == null) {
                DNFNodeToPush = new ArrayList<IFilter>();
            }

            DNFNodeToPush.addAll(DNFNode);
        }

        if (qtn instanceof QueryNode) {
            QueryNode qn = (QueryNode) qtn;
            QueryTreeNode child = pushFilter(qn.getChild(), DNFNodeToPush);
            if (canIgnore(qn)) {
                return child;
            } else {
                qn.setChild(child);
                return qn;
            }
        } else if (qtn instanceof JoinNode) {
            JoinNode jn = (JoinNode) qtn;
            List<IFilter> DNFNodetoPushToLeft = new LinkedList<IFilter>();
            List<IFilter> DNFNodetoPushToRight = new LinkedList<IFilter>();

            if (DNFNodeToPush != null) {
                DNFNodeToPush = addJoinKeysFromDNFNodeAndRemoveIt(DNFNodeToPush, jn);
                for (IFilter node : DNFNodeToPush) {
                    ISelectable c = (ISelectable) ((IBooleanFilter) node).getColumn();
                    // 下推到左右节点
                    if (jn.getLeftNode().hasColumn(c)) {
                        DNFNodetoPushToLeft.add(node);
                    } else if (jn.getRightNode().hasColumn(c)) {
                        DNFNodetoPushToRight.add(node);
                    } else {
                        assert (false);// 不可能出现
                    }
                }

                // 将左条件的表达式，推导到join filter的右条件上
                DNFNodetoPushToRight.addAll(copyFilterToJoinOnColumns(DNFNodeToPush,
                    jn.getLeftKeys(),
                    jn.getRightKeys()));

                // 将右条件的表达式，推导到join filter的左条件上
                DNFNodetoPushToLeft.addAll(copyFilterToJoinOnColumns(DNFNodeToPush, jn.getRightKeys(), jn.getLeftKeys()));
            }

            if (jn.isInnerJoin()) {
                jn.setLeftNode(pushFilter(jn.getLeftNode(), DNFNodetoPushToLeft));
                jn.setRightNode(pushFilter(((JoinNode) qtn).getRightNode(), DNFNodetoPushToRight));
            } else if (jn.isLeftOuterJoin()) {
                jn.setLeftNode(pushFilter(jn.getLeftNode(), DNFNodetoPushToLeft));
                if (DNFNodeToPush != null && !DNFNodeToPush.isEmpty()) {
                    jn.query(FilterUtils.DNFToAndLogicTree(DNFNodetoPushToRight)); // 在父节点完成filter，不能下推
                }
            } else if (jn.isRightOuterJoin()) {
                jn.setRightNode(pushFilter(((JoinNode) qtn).getRightNode(), DNFNodetoPushToRight));
                if (DNFNodeToPush != null && !DNFNodeToPush.isEmpty()) {
                    jn.query(FilterUtils.DNFToAndLogicTree(DNFNodetoPushToLeft));// 在父节点完成filter，不能下推
                }
            } else {
                if (DNFNodeToPush != null && !DNFNodeToPush.isEmpty()) {
                    jn.query(FilterUtils.DNFToAndLogicTree(DNFNodeToPush));
                }
            }

            jn.build();
            return jn;
        }

        return qtn;
    }

    /**
     * 约束条件应该尽量提前，针对join条件中的非join column列，比如column = 1的常量关系
     * 
     * <pre>
     * 如： tabl1.join(table2).on("table1.id>10&&table2.id<5") 
     * 优化成: able1.query("table1.id>10").join(table2.query("table2.id<5")) t但如果条件中包含
     * 
     * ||条件则暂不优化
     * </pre>
     */
    private static QueryTreeNode pushJoinOnFilter(QueryTreeNode qtn, List<IFilter> DNFNodeToPush) throws QueryException {
        if (qtn.getChildren().isEmpty()) {
            IFilter node = FilterUtils.DNFToAndLogicTree(DNFNodeToPush);
            if (node != null) {
                qtn.setOtherJoinOnFilter(FilterUtils.and(qtn.getOtherJoinOnFilter(), (IFilter) node.copy()));
                qtn.build();
            }
            return qtn;
        }

        // 对于 or连接的条件，就不能下推了
        IFilter filterInWhere = qtn.getOtherJoinOnFilter();
        if (filterInWhere != null && FilterUtils.isDNF(filterInWhere)) {
            List<IFilter> DNFNode = FilterUtils.toDNFNode(filterInWhere);
            if (DNFNodeToPush == null) {
                DNFNodeToPush = new ArrayList<IFilter>();
            }

            DNFNodeToPush.addAll(DNFNode);
        }
        if (qtn instanceof QueryNode) {
            QueryNode qn = (QueryNode) qtn;
            QueryTreeNode child = pushJoinOnFilter(qn.getChild(), DNFNodeToPush);
            if (canIgnore(qn)) {
                return child;
            } else {
                qn.setChild(child);
                return qn;
            }
        } else if (qtn instanceof JoinNode) {
            JoinNode jn = (JoinNode) qtn;
            List<IFilter> DNFNodetoPushToLeft = new LinkedList<IFilter>();
            List<IFilter> DNFNodetoPushToRight = new LinkedList<IFilter>();

            if (DNFNodeToPush != null) {
                for (IFilter node : DNFNodeToPush) {
                    ISelectable c = (ISelectable) ((IBooleanFilter) node).getColumn();
                    if (jn.getLeftNode().hasColumn(c)) {
                        DNFNodetoPushToLeft.add(node);
                    } else if (jn.getRightNode().hasColumn(c)) {
                        DNFNodetoPushToRight.add(node);
                    } else {
                        assert (false);
                    }
                }

                // 将左条件的表达式，推导到join filter的右条件上
                DNFNodetoPushToRight.addAll(copyFilterToJoinOnColumns(DNFNodeToPush,
                    jn.getLeftKeys(),
                    jn.getRightKeys()));

                // 将右条件的表达式，推导到join filter的左条件上
                DNFNodetoPushToLeft.addAll(copyFilterToJoinOnColumns(DNFNodeToPush, jn.getRightKeys(), jn.getLeftKeys()));
            }

            pushJoinOnFilter(jn.getLeftNode(), DNFNodetoPushToLeft);
            pushJoinOnFilter(jn.getRightNode(), DNFNodetoPushToRight);
            jn.build();
            return jn;
        }

        return qtn;
    }

    /**
     * 将连接列上的约束复制到目标节点内
     * 
     * @param DNF 要复制的DNF filter
     * @param other 要复制的目标节点
     * @param qnColumns 源节点的join字段
     * @param otherColumns 目标节点的join字段
     * @throws QueryException
     */
    private static List<IFilter> copyFilterToJoinOnColumns(List<IFilter> DNF, List<ISelectable> qnColumns,
                                                           List<ISelectable> otherColumns) throws QueryException {
        List<IFilter> newIFilterToPush = new LinkedList<IFilter>();
        for (IFilter bool : DNF) {
            int index = qnColumns.indexOf(((IBooleanFilter) bool).getColumn());
            if (index >= 0) {// 只考虑在源查找，在目标查找在上一层进行控制
                IBooleanFilter o = ASTNodeFactory.getInstance().createBooleanFilter().setOperation(bool.getOperation());
                o.setColumn(otherColumns.get(index));
                o.setValue(((IBooleanFilter) bool).getValue());
                newIFilterToPush.add(o);
            }
        }

        return newIFilterToPush;
    }

    /**
     * 将原本的Join的where条件中的a.id=b.id构建为join条件，并从where条件中移除
     */
    private static List<IFilter> addJoinKeysFromDNFNodeAndRemoveIt(List<IFilter> DNFNode, JoinNode join)
                                                                                                        throws QueryException {
        // filter中可能包含join列,如id=id
        // 目前必须满足以下条件
        // 1、不包含or
        // 2、=连接
        if (isFilterContainsColumnJoin(DNFNode)) {
            List<Object> leftJoinKeys = new ArrayList<Object>();
            List<Object> rightJoinKeys = new ArrayList<Object>();

            List<IFilter> filtersToRemove = new LinkedList();
            for (IFilter sub : DNFNode) { // 一定是简单条件
                ISelectable[] keys = getJoinKeysWithColumnJoin((IBooleanFilter) sub);
                if (keys != null) {// 存在join column
                    if (join.getLeftNode().hasColumn(keys[0])) {
                        if (!join.getLeftNode().hasColumn(keys[1])) {
                            if (join.getRightNode().hasColumn(keys[1])) {
                                leftJoinKeys.add(keys[0]);
                                rightJoinKeys.add(keys[1]);
                                filtersToRemove.add(sub);
                                join.addJoinFilter((IBooleanFilter) sub);
                            } else {
                                throw new IllegalArgumentException("join查询表右边不包含join column，请修改查询语句...");
                            }
                        } else {
                            throw new IllegalArgumentException("join查询的join column都在左表上，请修改查询语句...");
                        }
                    } else if (join.getLeftNode().hasColumn(keys[1])) {
                        if (!join.getLeftNode().hasColumn(keys[0])) {
                            if (join.getRightNode().hasColumn(keys[0])) {
                                leftJoinKeys.add(keys[1]);
                                rightJoinKeys.add(keys[0]);
                                filtersToRemove.add(sub);
                                // 交换一下
                                Object tmp = ((IBooleanFilter) sub).getColumn();
                                ((IBooleanFilter) sub).setColumn(((IBooleanFilter) sub).getValue());
                                ((IBooleanFilter) sub).setValue((Comparable) tmp);
                                join.addJoinFilter((IBooleanFilter) sub);
                            } else {
                                throw new IllegalArgumentException("join查询表左边不包含join column，请修改查询语句...");
                            }
                        } else {
                            throw new IllegalArgumentException("join查询的join column都在右表上，请修改查询语句...");
                        }
                    }
                }
            }

            DNFNode.removeAll(filtersToRemove);
        }

        join.build();
        return DNFNode;
    }

    private static boolean isFilterContainsColumnJoin(List<IFilter> DNFNode) {
        for (IFilter f : DNFNode) {
            if (f instanceof IBooleanFilter) {
                if (((IBooleanFilter) f).getColumn() instanceof IColumn
                    && ((IBooleanFilter) f).getValue() instanceof IColumn) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * 找到join的列条件的所有列信息，必须是a.id=b.id的情况，针对a.id=1返回为null
     */
    private static ISelectable[] getJoinKeysWithColumnJoin(IBooleanFilter filter) {
        if (((IBooleanFilter) filter).getColumn() instanceof IColumn
            && ((IBooleanFilter) filter).getValue() instanceof IColumn) {
            if (OPERATION.EQ.equals(filter.getOperation())) {
                return new ISelectable[] { (ISelectable) ((IBooleanFilter) filter).getColumn(),
                        (ISelectable) ((IBooleanFilter) filter).getValue() };
            }
        }

        return null;
    }

    /**
     * 判断一个QueryNode是否可以被删掉
     */
    private static boolean canIgnore(QueryNode qn) {
        if (qn.getChild() == null) {
            return false;
        }
        if (qn.getChild().isSubQuery()) {
            return false;
        }
        if (qn.getLimitFrom() != null || qn.getLimitTo() != null) {
            return false;
        }
        if (qn.getOrderBys() != null && !qn.getOrderBys().isEmpty()) {
            return false;
        }
        if (qn.getWhereFilter() != null) {
            return false;
        }

        return true;
    }
}
