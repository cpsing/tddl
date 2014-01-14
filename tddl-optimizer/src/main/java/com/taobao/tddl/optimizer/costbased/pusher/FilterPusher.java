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
import com.taobao.tddl.optimizer.utils.OptimizerUtils;

/**
 * 将filter进行下推
 * 
 * <pre>
 * a. 如果条件中包含||条件则暂不优化，下推时会导致语义不正确
 * b. 如果条件中的column/value包含function，也不做下推 (比较麻烦，需要递归处理函数中的字段信息，同时检查是否符合下推条件，先简答处理)
 * c. 如果条件中的column/value中的字段来自于子节点的函数查询，也不做下推
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
                qtn.query(FilterUtils.and(qtn.getWhereFilter(), node));
                qtn.build();
            }
            return qtn;
        }

        // 对于or连接的条件，就不能下推了
        IFilter filterInWhere = qtn.getWhereFilter();
        if (filterInWhere != null && FilterUtils.isCNFNode(filterInWhere)) {
            List DNFNode = FilterUtils.toDNFNode(filterInWhere);
            qtn.query((IFilter) null);// 清空where条件
            if (DNFNodeToPush == null) {
                DNFNodeToPush = new ArrayList<IFilter>();
            }

            DNFNodeToPush.addAll(OptimizerUtils.copyFilter(DNFNode));// 需要复制一份出来
        }

        if (qtn instanceof QueryNode) {
            QueryNode qn = (QueryNode) qtn;
            List<IFilter> DNFNodeToCurrent = new LinkedList<IFilter>();
            if (DNFNodeToPush != null) {
                for (IFilter node : DNFNodeToPush) {
                    // 可能是多级节点，字段在select中，设置为select中的字段，这样才可以继续下推
                    if (!tryPushColumn(node, qn.getChild())) {
                        // 可能where条件是函数，暂时不下推
                        DNFNodeToCurrent.add(node);
                    }
                }

                DNFNodeToPush.removeAll(DNFNodeToCurrent);
            }

            QueryTreeNode child = pushFilter(qn.getChild(), DNFNodeToPush);
            // 针对不能下推的，合并到当前的where
            IFilter node = FilterUtils.DNFToAndLogicTree(DNFNodeToCurrent);
            if (node != null) {
                qtn.query(FilterUtils.and(qtn.getWhereFilter(), node));
            }

            ((QueryNode) qtn).setChild(child);
            qtn.build();
        } else if (qtn instanceof JoinNode) {
            JoinNode jn = (JoinNode) qtn;
            List<IFilter> DNFNodetoPushToLeft = new LinkedList<IFilter>();
            List<IFilter> DNFNodetoPushToRight = new LinkedList<IFilter>();
            List<IFilter> DNFNodeToCurrent = new LinkedList<IFilter>();
            if (DNFNodeToPush != null) {
                // 需要处理不能下推的条件
                // 1. 处理a.id=b.id，左右两边都为column列
                // 2. 处理a.id = b.id + 1，一边为column，一边为function
                // 情况2这种不优化，直接当作where条件处理
                findJoinKeysAndRemoveIt(DNFNodeToPush, jn);
                for (IFilter node : DNFNodeToPush) {
                    if (tryPushColumn(node, jn.getLeftNode())) {
                        DNFNodetoPushToLeft.add(node);
                    } else if (tryPushColumn(node, jn.getRightNode())) {
                        DNFNodetoPushToRight.add(node);
                    } else {
                        // 可能是函数，不继续下推
                        DNFNodeToCurrent.add(node);
                    }
                }
                // 将左条件的表达式，推导到join filter的右条件上
                DNFNodetoPushToRight.addAll(copyFilterToJoinOnColumns(DNFNodeToPush,
                    jn.getLeftKeys(),
                    jn.getRightKeys()));

                // 将右条件的表达式，推导到join filter的左条件上
                DNFNodetoPushToLeft.addAll(copyFilterToJoinOnColumns(DNFNodeToPush, jn.getRightKeys(), jn.getLeftKeys()));
            }

            // 针对不能下推的，合并到当前的where
            IFilter node = FilterUtils.DNFToAndLogicTree(DNFNodeToCurrent);
            if (node != null) {
                qtn.query(FilterUtils.and(qtn.getWhereFilter(), node));
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
     * 优化成: able1.query("table1.id>10").join(table2.query("table2.id<5")) t但如果条件中包含||条件则暂不优化
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
        IFilter filterInOtherJoin = qtn.getOtherJoinOnFilter();
        if (filterInOtherJoin != null && FilterUtils.isCNFNode(filterInOtherJoin)) {
            // 需要复制，下推到子节点后，会改变column/value的tableName
            List<IFilter> DNFNode = FilterUtils.toDNFNode((IFilter) filterInOtherJoin.copy());
            if (DNFNodeToPush == null) {
                DNFNodeToPush = new ArrayList<IFilter>();
            }

            DNFNodeToPush.addAll(DNFNode);
        }

        if (qtn instanceof QueryNode) {
            QueryNode qn = (QueryNode) qtn;
            List<IFilter> DNFNodeToCurrent = new LinkedList<IFilter>();
            if (DNFNodeToPush != null) {
                // 如果是join/query/join，可能需要转一次select column，不然下推就会失败
                for (IFilter node : DNFNodeToPush) {
                    // 可能是多级节点，字段在select中，设置为select中的字段，这样才可以继续下推
                    // 因为query不可能是顶级节点，只会是传递的中间状态，不需要处理DNFNodeToCurrent
                    if (!tryPushColumn(node, qn.getChild())) {
                        // 可能where条件是函数，暂时不下推
                        DNFNodeToCurrent.add(node);
                    }
                }

                DNFNodeToPush.removeAll(DNFNodeToCurrent);
            }

            QueryTreeNode child = pushJoinOnFilter(qn.getChild(), DNFNodeToPush);
            // 针对不能下推的，合并到当前的where
            IFilter node = FilterUtils.DNFToAndLogicTree(DNFNodeToCurrent);
            if (node != null) {
                qtn.query(FilterUtils.and(qtn.getOtherJoinOnFilter(), (IFilter) node.copy()));
            }
            ((QueryNode) qtn).setChild(child);
            qtn.build();
            return qn;
        } else if (qtn instanceof JoinNode) {
            JoinNode jn = (JoinNode) qtn;
            List<IFilter> DNFNodetoPushToLeft = new LinkedList<IFilter>();
            List<IFilter> DNFNodetoPushToRight = new LinkedList<IFilter>();
            List<IFilter> DNFNodeToCurrent = new LinkedList<IFilter>();

            if (DNFNodeToPush != null) {
                for (IFilter node : DNFNodeToPush) {
                    if (tryPushColumn(node, jn.getLeftNode())) {
                        DNFNodetoPushToLeft.add(node);
                    } else if (tryPushColumn(node, jn.getRightNode())) {
                        DNFNodetoPushToRight.add(node);
                    } else {
                        // 可能是函数，不继续下推
                        DNFNodeToCurrent.add(node);
                    }
                }

                // 将左条件的表达式，推导到join filter的右条件上
                // 比如: table a left join table b on (a.id = b.id and b.id = 1)
                // 这时对应的b.id = 1的条件不能推导到左表，否则语义不对
                if (jn.isInnerJoin() || jn.isLeftOuterJoin()) {
                    DNFNodetoPushToRight.addAll(copyFilterToJoinOnColumns(DNFNodeToPush,
                        jn.getLeftKeys(),
                        jn.getRightKeys()));
                }

                if (jn.isInnerJoin() || jn.isRightOuterJoin()) {
                    // 将右条件的表达式，推导到join filter的左条件上
                    DNFNodetoPushToLeft.addAll(copyFilterToJoinOnColumns(DNFNodeToPush,
                        jn.getRightKeys(),
                        jn.getLeftKeys()));
                }
            }

            // 针对不能下推的，合并到当前的where，otherJoinOnFilter暂时不做清理，不需要做合并
            // IFilter node = FilterUtils.DNFToAndLogicTree(DNFNodeToCurrent);
            // if (node != null) {
            // qtn.setOtherJoinOnFilter(FilterUtils.and(qtn.getOtherJoinOnFilter(),
            // (IFilter) node.copy()));
            // }

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
                if (bool.getOperation() == OPERATION.IN) {
                    o.setValues(((IBooleanFilter) bool).getValues());
                } else {
                    o.setValue(((IBooleanFilter) bool).getValue());
                }
                newIFilterToPush.add(o);
            }
        }

        return newIFilterToPush;
    }

    /**
     * 将原本的Join的where条件中的a.id=b.id构建为join条件，并从where条件中移除
     */
    private static void findJoinKeysAndRemoveIt(List<IFilter> DNFNode, JoinNode join) throws QueryException {
        // filter中可能包含join列,如id=id
        // 目前必须满足以下条件
        // 1、不包含or
        // 2、=连接
        List<IFilter> joinFilters = new LinkedList();
        if (isFilterContainsColumnJoin(DNFNode)) {
            List<Object> leftJoinKeys = new ArrayList<Object>();
            List<Object> rightJoinKeys = new ArrayList<Object>();

            for (IFilter sub : DNFNode) { // 一定是简单条件
                ISelectable[] keys = getJoinKeysWithColumnJoin((IBooleanFilter) sub);
                if (keys != null) {// 存在join column
                    if (join.getLeftNode().hasColumn(keys[0])) {
                        if (!join.getLeftNode().hasColumn(keys[1])) {
                            if (join.getRightNode().hasColumn(keys[1])) {
                                leftJoinKeys.add(keys[0]);
                                rightJoinKeys.add(keys[1]);
                                joinFilters.add(sub);
                                join.addJoinFilter((IBooleanFilter) sub);
                            } else {
                                throw new IllegalArgumentException("join查询表右边不包含join column，请修改查询语句...");
                            }
                        } else {
                            if (!(join.getLeftNode() instanceof JoinNode)) {
                                throw new IllegalArgumentException("join查询的join column都在左表上，请修改查询语句...");
                            }
                        }
                    } else if (join.getLeftNode().hasColumn(keys[1])) {
                        if (!join.getLeftNode().hasColumn(keys[0])) {
                            if (join.getRightNode().hasColumn(keys[0])) {
                                leftJoinKeys.add(keys[1]);
                                rightJoinKeys.add(keys[0]);
                                joinFilters.add(sub);
                                // 交换一下
                                Object tmp = ((IBooleanFilter) sub).getColumn();
                                ((IBooleanFilter) sub).setColumn(((IBooleanFilter) sub).getValue());
                                ((IBooleanFilter) sub).setValue(tmp);
                                join.addJoinFilter((IBooleanFilter) sub);
                            } else {
                                throw new IllegalArgumentException("join查询表左边不包含join column，请修改查询语句...");
                            }
                        } else {
                            if (!(join.getRightNode() instanceof JoinNode)) {
                                throw new IllegalArgumentException("join查询的join column都在右表上，请修改查询语句...");
                            }
                        }
                    }
                }
            }

            DNFNode.removeAll(joinFilters);
        }

        join.build();
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
        if (filter.getColumn() instanceof IColumn && filter.getValue() instanceof IColumn) {
            if (OPERATION.EQ.equals(filter.getOperation())) {
                return new ISelectable[] { (ISelectable) filter.getColumn(), (ISelectable) filter.getValue() };
            }
        }

        return null;
    }

    /**
     * 尝试推一下column到子节点，会设置为查找到子节点上的column<br/>
     * 比如需要下推字段，可能来自于子节点的select，所以需要先转化为子节点上的select信息，再下推
     */
    private static boolean tryPushColumn(IFilter filter, QueryTreeNode qtn) {
        return tryPushColumn(filter, true, qtn) && tryPushColumn(filter, false, qtn);
    }

    private static boolean tryPushColumn(IFilter filter, boolean isColumn, QueryTreeNode qtn) {
        Object value = null;
        if (isColumn) {
            value = ((IBooleanFilter) filter).getColumn();
        } else {
            value = ((IBooleanFilter) filter).getValue();
        }

        if (value instanceof ISelectable) {
            ISelectable c = qtn.findColumn((ISelectable) value);
            if (c instanceof IColumn) {
                if (isColumn) {
                    ((IBooleanFilter) filter).setColumn(c.copy());
                } else {
                    ((IBooleanFilter) filter).setValue(c.copy());
                }
                return true;
            } else {
                return false;
            }
        } else {
            return true;
        }
    }
}
