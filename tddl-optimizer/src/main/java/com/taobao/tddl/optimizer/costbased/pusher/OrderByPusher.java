package com.taobao.tddl.optimizer.costbased.pusher;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.query.JoinNode;
import com.taobao.tddl.optimizer.core.ast.query.MergeNode;
import com.taobao.tddl.optimizer.core.ast.query.QueryNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.plan.query.IJoin.JoinStrategy;

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
 * 下推例子：
 * 1. 
 *  父节点：order by c1 ,c2 ,c3
 *  子节点: order by c1, c2
 *  优化：下推c3
 * 
 * 2. 
 *  父节点：order by c2 ,c3  (顺序不一致，下推也没效果，反而增加负担)
 *  子节点: order by c1, c2
 *  优化：不下推
 * 
 * 3. 
 *  父节点：order by c1, c2 ,c3
 *  子节点: 无
 *  优化：下推c1,c2,c3
 * 
 * 4. 
 *  父节点：order by count(*)  (函数不下推)
 *  子节点: 无
 *  优化：不下推
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
     * 处理Merge节点的distinct处理，需要底下节点先做排序
     * 
     * <pre>
     * 排序优先级：
     * groupBy > orderby > distinct
     * </pre>
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
                    ((QueryTreeNode) child).build();
                }
            }
        } else if (qtn instanceof JoinNode) {
            // index nested loop中的order by，可以推到左节点
            JoinNode join = (JoinNode) qtn;
            if (join.getJoinStrategy() == JoinStrategy.INDEX_NEST_LOOP) {
                List<IOrderBy> orders = getPushOrderBys(join, join.getLeftNode());
                if (orders != null && !orders.isEmpty()) {
                    for (IOrderBy order : orders) {
                        if (join.getLeftNode().hasColumn(order.getColumn())) {
                            join.getLeftNode().orderBy(order.getColumn(), order.getDirection());
                        } else if (join.isUedForIndexJoinPK()) {
                            // 尝试忽略下表名查找一下
                            ISelectable newC = order.getColumn().copy().setTableName(null);
                            if (join.getLeftNode().hasColumn(newC)) {
                                join.getLeftNode().orderBy(order.getColumn(), order.getDirection());
                            }
                        }
                    }
                    join.getLeftNode().build();
                }
            }
        } else if (qtn instanceof QueryNode) {
            // 可以将order推到子查询
            QueryNode query = (QueryNode) qtn;
            List<IOrderBy> orders = getPushOrderBys(query, query.getChild());
            if (orders != null && !orders.isEmpty()) {
                for (IOrderBy order : orders) {
                    query.getChild().orderBy(order.getColumn(), order.getDirection());
                }
            }

            query.build();
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

    /**
     * 尝试对比父节点中的orderby和子节点的orderby顺序，如果前缀一致，则找出末尾的order by字段进行返回
     * 
     * <pre>
     * 比如 
     * 1. 
     *  父节点：order by c1 ,c2 ,c3
     *  子节点: order by c1, c2
     *  
     * 返回为c3
     * 
     * 2. 
     *  父节点：order by c2 ,c3  (顺序不一致，下推也没效果，反而增加负担)
     *  子节点: order by c1, c2
     *  
     * 返回为空
     * 
     * 3. 
     *  父节点：order by c1, c2 ,c3
     *  子节点: 无
     *  
     * 返回为c1,c2,c3
     * 
     * 4. 
     *  父节点：order by count(*)  (函数不下推)
     *  子节点: 无
     * 
     * 返回空
     * </pre>
     */
    private static List<IOrderBy> getPushOrderBys(QueryTreeNode qtn, QueryTreeNode child) {
        List<IOrderBy> newOrderBys = new LinkedList<IOrderBy>();
        List<IOrderBy> implicitOrderBys = qtn.getImplicitOrderBys();
        List<IOrderBy> targetOrderBys = child.getOrderBys();
        if (implicitOrderBys == null || implicitOrderBys.size() == 0) {
            return new LinkedList<IOrderBy>();
        }
        for (int i = 0; i < implicitOrderBys.size(); i++) {
            IOrderBy order = implicitOrderBys.get(i);
            ISelectable column = qtn.findColumn(order.getColumn());// 找到select或者是meta中的字段
            if (!(column != null && column instanceof IColumn)) {
                // 可能orderby的字段为当前select的函数列
                return new LinkedList<IOrderBy>();
            }

            // 在子节点中找一次，转化为子节点中的字段信息，比如表名，这样才可以和orderby字段做比较
            column = child.findColumn(column);
            if (!(column != null && column instanceof IColumn)) {
                // 可能orderby的字段为当前select的函数列
                return new LinkedList<IOrderBy>();
            }

            if (targetOrderBys != null && targetOrderBys.size() > i) {
                IOrderBy targetOrder = targetOrderBys.get(i);
                if (!(column.equals(targetOrder.getColumn()) && order.getDirection().equals(targetOrder.getDirection()))) {// 如果不相同
                    return new LinkedList<IOrderBy>();
                }
            } else {
                // 如果出现父类的长度>子类
                IOrderBy newOrder = order.copy().setColumn(column.copy().setAlias(null));
                newOrderBys.add(newOrder);
            }
        }

        return newOrderBys;
    }
}
