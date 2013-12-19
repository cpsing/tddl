package com.taobao.tddl.optimizer.costbased.pusher;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.query.JoinNode;
import com.taobao.tddl.optimizer.core.ast.query.MergeNode;
import com.taobao.tddl.optimizer.core.ast.query.QueryNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.plan.query.IJoin.JoinStrategy;
import com.taobao.tddl.optimizer.exceptions.OptimizerException;

/**
 * 将merge/join中的order by条件下推，包括隐式的order by条件，比如将groupBy转化为orderBy
 * 
 * <pre>
 * a. 如果orderBy中包含function，也不做下推
 * b. 如果orderBy中的的column字段来自于子节点的函数查询，也不做下推
 * c. 如果join是SortMergeJoin，会下推join列
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
                    // 重新构建order by / group by字段
                    if (!child.getGroupBys().isEmpty()) {
                        List<IOrderBy> implicitOrderBys = child.getImplicitOrderBys();
                        // 如果order by包含了所有的group by顺序
                        if (containAllGroupBys(implicitOrderBys, child.getGroupBys())) {
                            child.setOrderBys(implicitOrderBys);
                        } else {
                            child.setOrderBys(child.getGroupBys());
                        }
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

                    // 清空child的group by，由merge节点进行处理
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
            if (join.getJoinStrategy() == JoinStrategy.INDEX_NEST_LOOP
                || join.getJoinStrategy() == JoinStrategy.NEST_LOOP_JOIN) {
                List<IOrderBy> orders = getPushOrderBys(join, join.getImplicitOrderBys(), join.getLeftNode());
                pushJoinOrder(orders, join.getLeftNode(), join.isUedForIndexJoinPK());
            } else if (join.getJoinStrategy() == JoinStrategy.SORT_MERGE_JOIN) {
                // 如果是sort merge join中的order by，需要推到左/右节点
                List<IOrderBy> implicitOrders = join.getImplicitOrderBys();
                Map<ISelectable, IOrderBy> orderColumnsMap = new HashMap<ISelectable, IOrderBy>();
                for (IOrderBy order : implicitOrders) {
                    orderColumnsMap.put(order.getColumn(), order);
                }

                List<IOrderBy> leftJoinColumnOrderbys = new ArrayList<IOrderBy>();
                List<IOrderBy> rightJoinColumnOrderbys = new ArrayList<IOrderBy>();
                for (IBooleanFilter joinFilter : join.getJoinFilter()) {
                    ISelectable column = (ISelectable) joinFilter.getColumn();
                    ISelectable value = (ISelectable) joinFilter.getValue();
                    if (!(column instanceof IColumn && value instanceof IColumn)) {
                        throw new OptimizerException("join列出现函数列,下推函数orderby过于复杂,此时sort merge join无法支持");
                    }

                    // 复制下隐藏orderby的asc/desc
                    boolean asc = true;
                    IOrderBy o = orderColumnsMap.get(column);
                    if (o != null) {
                        asc = o.getDirection();
                    }

                    o = orderColumnsMap.get(value);
                    if (o != null) {
                        asc = o.getDirection();
                    }

                    leftJoinColumnOrderbys.add(ASTNodeFactory.getInstance()
                        .createOrderBy()
                        .setColumn(column)
                        .setDirection(asc));
                    rightJoinColumnOrderbys.add(ASTNodeFactory.getInstance()
                        .createOrderBy()
                        .setColumn(value)
                        .setDirection(asc));
                }
                // 调整下join orderBys的顺序，尽可能和原始的order by顺序一致，这样可以有利于下推
                adjustJoinColumnByImplicitOrders(leftJoinColumnOrderbys, rightJoinColumnOrderbys, implicitOrders);
                // 先推join列，
                // 如果join列推失败了，比如join列是函数列，这问题就蛋疼了，需要提前做判断
                List<IOrderBy> leftOrders = getPushOrderBys(join, leftJoinColumnOrderbys, join.getLeftNode());
                pushJoinOrder(leftOrders, join.getLeftNode(), join.isUedForIndexJoinPK());

                List<IOrderBy> rightOrders = getPushOrderBys(join, rightJoinColumnOrderbys, join.getRightNode());
                pushJoinOrder(rightOrders, join.getRightNode(), join.isUedForIndexJoinPK());
                if (!implicitOrders.isEmpty()) {
                    // group by + order by的隐藏列，如果和join列前缀相同，则下推，否则忽略
                    leftOrders = getPushOrderBys(join, implicitOrders, join.getLeftNode());
                    rightOrders = getPushOrderBys(join, implicitOrders, join.getRightNode());
                    if (!leftOrders.isEmpty() || !rightOrders.isEmpty()) {
                        pushJoinOrder(leftOrders, join.getLeftNode(), join.isUedForIndexJoinPK());
                        pushJoinOrder(rightOrders, join.getRightNode(), join.isUedForIndexJoinPK());
                    } else {
                        // 尝试一下只推group by的排序，减少一层排序
                        if (join.getGroupBys() != null && !join.getGroupBys().isEmpty()) {
                            List<IOrderBy> leftImplicitOrders = getPushOrderBysCombileGroupAndJoinColumns(join.getGroupBys(),
                                leftJoinColumnOrderbys);
                            leftOrders = getPushOrderBys(join, leftImplicitOrders, join.getLeftNode());

                            List<IOrderBy> rightImplicitOrders = getPushOrderBysCombileGroupAndJoinColumns(join.getGroupBys(),
                                rightJoinColumnOrderbys);

                            // 重置下group by的顺序
                            if (!leftImplicitOrders.isEmpty()) {
                                join.setGroupBys(leftImplicitOrders);
                            } else if (!rightImplicitOrders.isEmpty()) {
                                join.setGroupBys(rightImplicitOrders);
                            }

                            rightOrders = getPushOrderBys(join, rightImplicitOrders, join.getRightNode());
                            if (!leftOrders.isEmpty() || !rightOrders.isEmpty()) {
                                pushJoinOrder(leftOrders, join.getLeftNode(), join.isUedForIndexJoinPK());
                                pushJoinOrder(rightOrders, join.getRightNode(), join.isUedForIndexJoinPK());

                            }
                        }
                    }
                }
            }

            join.build();
        } else if (qtn instanceof QueryNode) {
            // 可以将order推到子查询
            QueryNode query = (QueryNode) qtn;
            List<IOrderBy> orders = getPushOrderBys(query, query.getImplicitOrderBys(), query.getChild());
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

    private static boolean containAllGroupBys(List<IOrderBy> orderBys, List<IOrderBy> groupBys) {
        for (IOrderBy group : groupBys) {
            boolean found = false;
            for (IOrderBy order : orderBys) {
                if (order.getColumn().equals(group.getColumn())) {
                    found = true;
                    break;
                }
            }

            if (!found) {
                return false;
            }
        }

        return true;
    }

    /**
     * 调整join列，按照order by的顺序，有利于下推
     */
    private static void adjustJoinColumnByImplicitOrders(List<IOrderBy> leftJoinOrders, List<IOrderBy> rightJoinOrders,
                                                         List<IOrderBy> implicitOrders) {
        // 调整下join orderBys的顺序，尽可能和原始的order by顺序一致，这样可以有利于下推
        for (int i = 0; i < implicitOrders.size(); i++) {
            if (i >= leftJoinOrders.size()) {
                return;
            }
            IOrderBy order = implicitOrders.get(i);
            int leftIndex = findOrderByByColumn(leftJoinOrders, order.getColumn());
            int rightIndex = findOrderByByColumn(rightJoinOrders, order.getColumn());

            int index = -1;
            if (leftIndex >= 0 && leftIndex != i) { // 判断位置是否相同
                index = leftIndex;
            } else if (rightIndex >= 0 && rightIndex != i) {
                index = rightIndex;
            }

            if (index >= 0) {
                // 交换位置一下
                IOrderBy tmp = leftJoinOrders.get(i);
                leftJoinOrders.set(i, leftJoinOrders.get(index));
                leftJoinOrders.set(index, tmp);

                tmp = rightJoinOrders.get(i);
                rightJoinOrders.set(i, rightJoinOrders.get(index));
                rightJoinOrders.set(index, tmp);
            } else {
                return;// join列中没有order by的字段，直接退出
            }
        }
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
    private static List<IOrderBy> getPushOrderBys(QueryTreeNode qtn, List<IOrderBy> implicitOrderBys,
                                                  QueryTreeNode child) {
        List<IOrderBy> newOrderBys = new LinkedList<IOrderBy>();
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

    /**
     * 尝试组合group by和join列的排序字段，以join列顺序为准，重排groupBys顺序
     */
    private static List<IOrderBy> getPushOrderBysCombileGroupAndJoinColumns(List<IOrderBy> groups,
                                                                            List<IOrderBy> joinOrders) {
        List<IOrderBy> newOrderbys = new ArrayList<IOrderBy>();
        for (IOrderBy joinOrder : joinOrders) {
            if (findOrderByByColumn(groups, joinOrder.getColumn()) >= 0) {
                newOrderbys.add(joinOrder);
            } else {
                return new ArrayList<IOrderBy>(); // 返回一般的顺序没用
            }
        }

        for (IOrderBy group : groups) {
            // 找到join column中没有的进行添加
            if (findOrderByByColumn(newOrderbys, group.getColumn()) < 0) {
                newOrderbys.add(group);
            }
        }

        return newOrderbys;
    }

    /**
     * 尝试查找一个同名的排序字段，返回下标，-1代表没找到
     */
    protected static int findOrderByByColumn(List<IOrderBy> orderbys, ISelectable column) {
        for (int i = 0; i < orderbys.size(); i++) {
            IOrderBy order = orderbys.get(i);
            if (order.getColumn().equals(column)) {
                return i;
            }
        }

        return -1;
    }

    private static void pushJoinOrder(List<IOrderBy> orders, QueryTreeNode qn, boolean isUedForIndexJoinPK) {
        if (orders != null && !orders.isEmpty()) {
            for (IOrderBy order : orders) {
                if (qn.hasColumn(order.getColumn())) {
                    qn.orderBy(order.getColumn(), order.getDirection());
                } else if (isUedForIndexJoinPK) {
                    // 尝试忽略下表名查找一下
                    ISelectable newC = order.getColumn().copy().setTableName(null);
                    if (qn.hasColumn(newC)) {
                        qn.orderBy(order.getColumn(), order.getDirection());
                    }
                }
            }

            qn.build();
        }
    }
}
