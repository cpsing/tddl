package com.taobao.tddl.optimizer.costbased.pusher;

import java.util.ArrayList;
import java.util.List;

import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.query.JoinNode;
import com.taobao.tddl.optimizer.core.ast.query.MergeNode;
import com.taobao.tddl.optimizer.core.ast.query.QueryNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.ast.query.strategy.IndexNestedLoopJoin;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;

/**
 * 将merge/join中的order by条件下推，包括隐式的order by条件，比如将groupBy转化为orderBy
 * 
 * <pre>
 * a. 如果orderBy中包含function，也不做下推
 * b. 如果orderBy中的的column字段来自于子节点的函数查询，也不做下推
 * 
 * 比如: tabl1.join(table2).on("table1.id=table2.id").orderBy("id")
 * 转化为：table.orderBy(id).join(table2).on("table1.id=table2.id")
 * 
 * </pre>
 * 
 * @author jianghang 2013-12-10 下午5:33:16
 * @since 5.1.0
 */
public class OrderByPusher {

    /**
     * 详细优化见类描述 {@linkplain OrderByPusher}
     */
    public static QueryTreeNode optimize(QueryTreeNode qtn) {
        qtn = optimizeDistinct(qtn);
        qtn = pushOrderBy(qtn);
        return qtn;
    }

    /**
     * @param qtn
     * @return
     */
    private static QueryTreeNode optimizeDistinct(QueryTreeNode qtn) {
        for (ASTNode child : ((QueryTreeNode) qtn).getChildren()) {
            if (child instanceof QueryTreeNode) {
                optimizeDistinct((QueryTreeNode) child);
            }
        }

        if (qtn instanceof MergeNode) {
            MergeNode merge = (MergeNode) qtn;
            if (!(merge.getChild() instanceof QueryTreeNode)) {
                return merge;
            }

            if (containsDistinctColumns(merge)) {
                for (ASTNode con : merge.getChildren()) {
                    QueryTreeNode child = (QueryTreeNode) con;
                    List<IFunction> toRemove = new ArrayList();
                    for (ISelectable s : child.getColumnsSelected()) {
                        if (s instanceof IFunction) {
                            toRemove.add((IFunction) s);
                        }
                    }
                    // 删除聚合函数/group by，由merge节点进行处理
                    child.getColumnsSelected().removeAll(toRemove);

                    // distinct group by同时存在时，要先安group by的列排序
                    if (child.getGroupBys() != null && !child.getGroupBys().isEmpty()) {
                        child.setOrderBys(child.getGroupBys());
                    }

                    // 将查询所有字段进行order by，保证每个child返回的数据顺序都是一致的
                    for (ISelectable s : child.getColumnsSelected()) {
                        boolean existed = false;
                        for (IOrderBy orderExisted : child.getOrderBys()) {
                            if (orderExisted.getColumn().equals(s)) {
                                existed = true;
                                break;
                            }
                        }

                        if (!existed) {
                            IOrderBy order = ASTNodeFactory.getInstance().createOrderBy();
                            order.setColumn(s).setDirection(true);
                            child.orderBy(s, true);
                        }
                    }

                    child.setGroupBys(new ArrayList(0));
                }

                // 设置为distinct标记
                for (ISelectable s : merge.getColumnsSelected()) {
                    if (s instanceof IFunction && isDistinct(s)) {
                        ((IFunction) s).setNeedDistinctArg(true);
                    }
                }

                return merge;
            }
        }

        return qtn;
    }

    private static QueryTreeNode pushOrderBy(QueryTreeNode qtn) {
        if (qtn instanceof MergeNode) {
            MergeNode merge = (MergeNode) qtn;
            if (!(merge.getChild() instanceof QueryTreeNode)) {
                return qtn;
            }

            if (merge.isUnion()) {
                List<IOrderBy> standardOrder = ((QueryTreeNode) merge.getChild()).getImplicitOrderBys();
                for (ASTNode child : merge.getChildren()) {
                    ((QueryTreeNode) child).setOrderBys(new ArrayList(0));

                    // 优先以主键为准
                    if (child instanceof TableNode) {
                        if (((TableNode) child).getIndexUsed().isPrimaryKeyIndex()) {
                            standardOrder = ((TableNode) child).getImplicitOrderBys();
                        }
                    }
                }

                for (ASTNode child : merge.getChildren()) {
                    ((QueryTreeNode) child).setOrderBys(standardOrder);
                }
            }
        } else if (qtn instanceof JoinNode) {
            // index nested loop中的order by，可以推到左节点
            JoinNode join = (JoinNode) qtn;
            if (join.getJoinStrategy() instanceof IndexNestedLoopJoin) {
                List<IOrderBy> orders = join.getImplicitOrderBys();
                if (orders != null && !orders.isEmpty()) {
                    for (IOrderBy order : orders) {
                        if (join.getLeftNode().hasColumn(order.getColumn())) {
                            join.getLeftNode().orderBy(order.getColumn().copy(), order.getDirection());
                        } else if (join.isUedForIndexJoinPK()) {
                            // 尝试忽略下表名查找一下
                            ISelectable newC = order.getColumn().copy().setTableName(null);
                            if (join.getLeftNode().hasColumn(newC)) {
                                join.getLeftNode().orderBy(newC, order.getDirection());
                            }
                        }
                    }
                    join.getLeftNode().build();
                }
            }
        } else if (qtn instanceof QueryNode) {
            // TODO 子查询
        }

        for (ASTNode child : ((QueryTreeNode) qtn).getChildren()) {
            if (child instanceof QueryTreeNode) {
                pushOrderBy((QueryTreeNode) child);
            }
        }

        return qtn;
    }

    /**
     * 判断是否存在distinct函数
     */
    private static boolean containsDistinctColumns(QueryTreeNode qc) {
        for (ISelectable c : qc.getColumnsSelected()) {
            if (isDistinct(c)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isDistinct(ISelectable s) {
        if (s.isDistinct()) {
            return true;
        }

        if (s instanceof IFunction) {
            for (Object arg : ((IFunction) s).getArgs()) {
                if (arg instanceof ISelectable) {
                    if (isDistinct((ISelectable) arg)) {
                        return true;
                    }
                }
            }
        }

        return false;
    }
}
