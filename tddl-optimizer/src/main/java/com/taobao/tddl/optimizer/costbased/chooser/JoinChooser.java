package com.taobao.tddl.optimizer.costbased.chooser;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.model.ExtraCmd;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode.FilterType;
import com.taobao.tddl.optimizer.core.ast.query.JoinNode;
import com.taobao.tddl.optimizer.core.ast.query.MergeNode;
import com.taobao.tddl.optimizer.core.ast.query.QueryNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.ILogicalFilter;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.plan.query.IJoin.JoinStrategy;
import com.taobao.tddl.optimizer.costbased.FilterSpliter;
import com.taobao.tddl.optimizer.costbased.esitimater.Cost;
import com.taobao.tddl.optimizer.costbased.esitimater.CostEsitimaterFactory;
import com.taobao.tddl.optimizer.costbased.pusher.OrderByPusher;
import com.taobao.tddl.optimizer.exceptions.QueryException;
import com.taobao.tddl.optimizer.utils.FilterUtils;

/**
 * 优化一下join
 * 
 * <pre>
 * 优化策略：
 * 1. 选择合适的索引
 * 2. 基于选择的索引，拆分where条件为key/indexValue/Result filter
 * 3. 针对不至此or条件的引擎，拆分OR为两个TableNode，进行merge查询. (需要考虑重复数据去重)
 * 4. 选择join策略
 *     如果内表是一个TableNode，或者是一个紧接着TableNode的QueryNode
 *     先只考虑约束条件，而不考虑Join条件分以下两种大类情况：
 *     1.内表没有选定索引(约束条件里没有用到索引的列)
 *       1.1内表进行Join的列不存在索引
 *          策略：NestLoop，内表使用全表扫描
 *       1.2内表进行Join的列存在索引
 *          策略：IndexNestLoop,在Join列里面选择索引，原本的全表扫描会分裂成一个Join
 *          
 *     2.内表已经选定了索引（KeyFilter存在）
 *       2.1内表进行Join的列不存在索引
 *          策略：NestLoop，内表使用原来的索引
 *       2.2内表进行Join的列存在索引
 *          这种情况最为复杂，有两种方法
 *              a. 放弃原来根据约束条件选择的索引，而使用Join列中得索引(如果有的话)，将约束条件全部作为ValueFilter，这样可以使用IndexNestLoop
 *              b. 采用根据约束条件选择的索引，而不管Join列，这样只能使用NestLoop
 *          如果内表经约束后的大小比较小，则可以使用方案二，反之，则应使用方案一,不过此开销目前很难估算。
 *          或者枚举所有可能的情况，貌似也比较麻烦，暂时只采用方案二，实现简单一些。
 * 5. 下推join/merge的order by条件
 * 
 * 如果设置了join节点顺序选择，会对可join的节点进行全排列，选择最合适的join，比如左表的数据最小
 * </pre>
 */
public class JoinChooser {

    /**
     * @param qtn
     * @param extraCmd
     * @return
     */
    public static QueryTreeNode optimize(QueryTreeNode qtn, Map<String, Object> extraCmd) {
        optimizeSubQuery(qtn, extraCmd);// 先遍历完成对QueryNode的子优化，因为这个优化不会在optimizeJoin处理
        qtn = optimizeJoin(qtn, extraCmd);
        qtn.build();
        return qtn;
    }

    /**
     * <pre>
     * 由于优化过程中需要将QueryTree转换为执行计划树，而外部查询转换为执行计划树是依赖于子查询的
     * 所以需要先对子查询进行优化，再对外层查询进行优化，回溯完成
     * </pre>
     */
    private static void optimizeSubQuery(QueryTreeNode qtn, Map<String, Object> extraCmd) throws QueryException {
        if (qtn instanceof QueryNode) {
            QueryNode qn = (QueryNode) qtn;
            if (qn.getChild() != null) {
                optimizeSubQuery(qn.getChild(), extraCmd);
                qn.setChild(JoinChooser.optimize(qn.getChild(), extraCmd));
            }
        } else if (qtn instanceof JoinNode) {
            JoinNode jn = (JoinNode) qtn;
            optimizeSubQuery(jn.getLeftNode(), extraCmd);
            optimizeSubQuery(jn.getRightNode(), extraCmd);
        } else {// table/merge
            if ((!qtn.getChildren().isEmpty()) && qtn.getChildren().get(0) != null) {
                optimizeSubQuery((QueryTreeNode) qtn.getChildren().get(0), extraCmd);
            }
        }

    }

    /**
     * 优化一棵查询树,以QueryNode为叶子终止,子节点单独进行优化
     */
    private static QueryTreeNode optimizeJoin(QueryTreeNode qtn, Map<String, Object> extraCmd) {
        // 暂时跳过可能存在的聚合函数
        if (!(qtn instanceof TableNode || qtn instanceof JoinNode)) {
            qtn.getChildren().set(0, optimize((QueryTreeNode) qtn.getChildren().get(0), extraCmd));
            return qtn;
        }

        boolean needReChooserJoinOrder = needReChooseJoinOrder(qtn);
        JoinPermutationGenerator jpg = null;
        if (needReChooserJoinOrder || isOptimizeJoinOrder(extraCmd)) {
            jpg = new JoinPermutationGenerator(qtn);
            qtn = jpg.getNext();
        }

        long minIo = Long.MAX_VALUE;
        QueryTreeNode minCostQueryTree = null;

        // 枚举每一种join的顺序，并计算开销，每次保留当前开销最小的Join次序
        while (qtn != null) {
            qtn = chooseStrategyAndIndexAndSplitQuery(qtn, extraCmd);
            qtn = qtn.convertToJoinIfNeed();
            qtn = OrderByPusher.optimize(qtn);
            if (isOptimizeJoinOrder(extraCmd)) {
                // 计算开销
                Cost cost = CostEsitimaterFactory.estimate(qtn);
                if (cost.getScanCount() < minIo) {
                    minIo = cost.getScanCount();
                    minCostQueryTree = qtn;
                }
                qtn = jpg.getNext();
            } else if (needReChooserJoinOrder) {
                qtn = jpg.getNext();
                needReChooserJoinOrder = false;
            } else {
                // 不需要进行join选择，直接退出
                minCostQueryTree = qtn;
                break;
            }

        }

        minCostQueryTree.build();
        return minCostQueryTree;
    }

    /**
     * 判断是否需要调整join顺序
     * 
     * <pre>
     * 比如mysql: select xx from a,b,c where a.id = c.id and b.name = c.name
     * 这时的结构树为 (a join b) join c ， a join b上不存在join条件，需要调整join顺序为 (a join c) join b 或者 (b join c) join a
     * </pre>
     */
    private static boolean needReChooseJoinOrder(QueryTreeNode qtn) {
        if (qtn instanceof JoinNode) {
            if (((JoinNode) qtn).getJoinFilter() == null || ((JoinNode) qtn).getJoinFilter().isEmpty()) {
                return true;
            }
        }

        for (ASTNode node : qtn.getChildren()) {
            if (!(node instanceof QueryTreeNode)) {
                return false;
            }

            if (needReChooseJoinOrder((QueryTreeNode) node)) {
                return true;
            }
        }

        return false;
    }

    /**
     * 遍历每个节点 分解Query 为Query选择索引与Join策略 只遍历一棵子查询树
     */
    private static QueryTreeNode chooseStrategyAndIndexAndSplitQuery(QueryTreeNode node, Map<String, Object> extraCmd)
                                                                                                                      throws QueryException {
        if (node instanceof JoinNode) {
            for (int i = 0; i < node.getChildren().size(); i++) {
                QueryTreeNode child = (QueryTreeNode) node.getChildren().get(i);
                child = chooseStrategyAndIndexAndSplitQuery(child, extraCmd);
                node.getChildren().set(i, child);
            }
        }

        if (node instanceof TableNode) {
            // Query是对实体表进行查询
            List<QueryTreeNode> ss = FilterSpliter.splitByDNF((TableNode) node, extraCmd);
            // 如果子查询中得某一个没有keyFilter，也即需要做全表扫描
            // 那么其他的子查询也没必要用索引了，都使用全表扫描即可
            // 直接把原来的约束条件作为valuefilter，全表扫描就行。
            boolean isExistAQueryNeedTableScan = false;
            for (QueryTreeNode s : ss) {
                if (s instanceof TableNode && ((TableNode) s).isFullTableScan()) {
                    isExistAQueryNeedTableScan = true;
                }
            }

            if (isExistAQueryNeedTableScan) {
                ((TableNode) node).setIndexQueryValueFilter(null);
                ((TableNode) node).setKeyFilter(null);
                ((TableNode) node).setResultFilter(null);
                ss.clear();
                ss = null;
                ((TableNode) node).setFullTableScan(true);
            }

            if (ss != null && ss.size() > 1) {
                MergeNode merge = new MergeNode();
                merge.alias(ss.get(0).getAlias());
                // limit操作在merge完成
                for (QueryTreeNode s : ss) {
                    merge.merge(s);
                }

                merge.setUnion(true);
                merge.build();
                return merge;

            } else if (ss != null && ss.size() == 1) {
                return ss.get(0);
            } else {
                // 出现了ss为null，即代表需要全表扫描
                // 查找可以包含所有选择列的索引
                IndexMeta indexWithAllColumnsSelected = IndexChooser.findBestIndexByAllColumnsSelected(((TableNode) node).getTableMeta(),
                    ((TableNode) node).getColumnsRefered(),
                    extraCmd);

                if (indexWithAllColumnsSelected != null) {
                    // 如果存在索引，则可以使用index value filter
                    ((TableNode) node).useIndex(indexWithAllColumnsSelected);
                    ((TableNode) node).setIndexQueryValueFilter(node.getWhereFilter());
                } else {
                    // 没有索引，则可以使用result filter
                    ((TableNode) node).setResultFilter(node.getWhereFilter());
                }

                return node;
            }
        } else if (node instanceof JoinNode) {
            // 如果右表是subquery，则也不能用indexNestedLoop
            if (((JoinNode) node).getRightNode().isSubQuery()) {
                ((JoinNode) node).setJoinStrategy(JoinStrategy.NEST_LOOP_JOIN);
                // 将未能下推的条件加到result filter中
                ((JoinNode) node).setResultFilter(FilterUtils.and(node.getResultFilter(), node.getWhereFilter()));
                return node;
            }

            /**
             * <pre>
             * 如果内表是一个TableNode，或者是一个紧接着TableNode的QueryNode
             * 考虑orderby条件和join类型
             * 1. 如果是outter join
             *      a. 存在order by
             *          i. join列是一个orderBy列的子集，并且是一个前序匹配，选择SortMergeJoin
             *          ii. 不满足条件i,需要做本地排序，按照join列, 选择SortMergeJoin
             *      b. 不存在order by,存在group by
             *          i. join列是一个orderBy列的子集，调整group by的顺序，选择SortMergeJoin
             *          ii. 不满足条件i,需要做本地排序，按照join列, 选择SortMergeJoin
             *      c.  不满足a和b时，选择SortMergeJoin，下推join列做为排序条件
             * 2. left outter/right outter join
             *      a. 对应的outter表上存在order by字段时，join列是一个orderBy列的子集，并且是一个前序匹配，选择SortMergeJoin
             *      b. 对应的outter表上存在group by字段时，join列是一个orderBy列的子集，调整groupBy顺序，选择SortMergeJoin
             * 
             * 其余case考虑约束条件，而不考虑Join条件分以下两种大类情况：
             * 1.内表没有选定索引(约束条件里没有用到索引的列)
             *   1.1内表进行Join的列不存在索引
             *      策略：NestLoop，内表使用全表扫描
             *   1.2内表进行Join的列存在索引
             *      策略：IndexNestLoop,在Join列里面选择索引，原本的全表扫描会分裂成一个Join
             *      
             * 2.内表已经选定了索引（KeyFilter存在）
             *   2.1内表进行Join的列不存在索引
             *      策略：NestLoop，内表使用原来的索引
             *   2.2内表进行Join的列存在索引
             *      这种情况最为复杂，有三种方法
             *          a. 如果join列和索引选择相同，这样可以使用IndexNestLoop
             *          b. 放弃原来根据约束条件选择的索引，而使用Join列中得索引(如果有的话)，将约束条件全部作为ValueFilter，这样可以使用IndexNestLoop
             *          c. 采用根据约束条件选择的索引，而不管Join列，这样只能使用NestLoop
             *      如果内表经约束后的大小比较小，则可以使用方案二，反之，则应使用方案一,不过此开销目前很难估算。
             *      或者枚举所有可能的情况，貌似也比较麻烦，暂时只采用方案二，实现简单一些。
             * </pre>
             */

            if (((JoinNode) node).isOuterJoin()) {
                // 几种分支都是选择sort merge join
                // join列的处理会在JoinNode.getImplicitOrderBys()中进行
                ((JoinNode) node).setJoinStrategy(JoinStrategy.SORT_MERGE_JOIN);
                return node;
            } else if (((JoinNode) node).isLeftOuterJoin() || ((JoinNode) node).isRightOuterJoin()) {
                if (canChooseSortMerge((JoinNode) node)) {
                    ((JoinNode) node).setJoinStrategy(JoinStrategy.SORT_MERGE_JOIN);
                    return node;
                }
            }

            QueryTreeNode innerNode = ((JoinNode) node).getRightNode();
            if (innerNode instanceof TableNode) {
                if (!((TableNode) innerNode).containsKeyFilter()) {
                    String tablename = ((TableNode) innerNode).getTableName();
                    if (innerNode.getAlias() != null) {
                        tablename = innerNode.getAlias();
                    }
                    // 找到对应join字段的索引
                    IndexMeta index = IndexChooser.findBestIndex((((TableNode) innerNode).getTableMeta()),
                        ((JoinNode) node).getRightKeys(),
                        Collections.<IFilter> emptyList(),
                        tablename,
                        extraCmd);
                    ((TableNode) innerNode).useIndex(index);
                    buildTableFilter(((TableNode) innerNode));

                    if (index == null) {// case 1.1
                        ((JoinNode) node).setJoinStrategy(JoinStrategy.NEST_LOOP_JOIN);
                    } else {// case 1.2
                        for (IBooleanFilter filter : ((JoinNode) node).getJoinFilter()) {
                            ISelectable rightColumn = (ISelectable) filter.getValue();
                            if (index.getKeyColumn(rightColumn.getColumnName()) == null) {
                                node.addResultFilter(filter); // 非索引的列
                            }
                        }

                        // 删除join条件中的非索引的列，将其做为where条件
                        if (node.getResultFilter() != null) {
                            if (node.getResultFilter() instanceof IBooleanFilter) {
                                ((JoinNode) node).getJoinFilter().remove(node.getResultFilter());
                            } else {
                                ((JoinNode) node).getJoinFilter()
                                    .removeAll(((ILogicalFilter) ((JoinNode) node).getResultFilter()).getSubFilter());
                            }

                            node.build();
                        }

                        ((JoinNode) node).setJoinStrategy(JoinStrategy.INDEX_NEST_LOOP);
                    }
                } else {
                    IndexMeta index = ((TableNode) innerNode).getIndexUsed();// 一定存在
                    boolean isCover = true;
                    for (IBooleanFilter filter : ((JoinNode) node).getJoinFilter()) {
                        ISelectable rightColumn = (ISelectable) filter.getValue();
                        if (index.getKeyColumn(rightColumn.getColumnName()) == null) {
                            isCover = false; // join中出现index没有的列
                        }
                    }

                    if (isCover) {
                        // case 2.2中的a
                        ((JoinNode) node).setJoinStrategy(JoinStrategy.INDEX_NEST_LOOP);
                    } else {
                        // case 2，因为2.1与2.2现在使用同一种策略，就是使用NestLoop...
                        ((JoinNode) node).setJoinStrategy(JoinStrategy.NEST_LOOP_JOIN);
                    }
                }

            } else { // 这种情况也属于case 2，先使用NestLoop...
                ((JoinNode) node).setJoinStrategy(JoinStrategy.NEST_LOOP_JOIN);
            }

            // 将未能下推的条件加到result filter中
            ((JoinNode) node).setResultFilter(FilterUtils.and(node.getResultFilter(), node.getWhereFilter()));
            return node;
        } else {
            // 将未能下推的条件加到result filter中
            node.setResultFilter(FilterUtils.and(node.getResultFilter(), node.getWhereFilter()));
            return node;
        }
    }

    /**
     * <pre>
     * 选择sort merge join的条件：
     * 1. 存在orderby，orderby顺序包含所有join列或者join列包含所有orderby字段，一个前缀顺序匹配. (orderby必须按顺序匹配，join列可以无序)
     * 2. 存在groupby, groupby中包含所有join列，或者join列包含所有groupby，(group by和join列都可以无序)
     * </pre>
     */
    private static boolean canChooseSortMerge(JoinNode node) {
        List<ISelectable> columns = node.isLeftOuterJoin() ? node.getRightKeys() : node.getLeftKeys();
        // 先判断group，判断排序条件是否满足
        // 再判断order
        return match(node.getGroupBys(), columns, false) || match(node.getImplicitOrderBys(), columns, true);
    }

    private static boolean match(List<IOrderBy> orderBys, List<ISelectable> columns, boolean needOrder) {
        if (orderBys.isEmpty()) {
            return false; // 这种情况就用传统的模式
        }

        if (needOrder) {
            for (int i = 0; i < orderBys.size(); i++) {
                IOrderBy order = orderBys.get(i);
                if (i >= columns.size()) {
                    return true; // 代表前缀匹配成功
                }

                boolean match = false;
                for (ISelectable column : columns) {
                    if (order.getColumn().equals(column)) {
                        match = true;
                        break;
                    }
                }

                if (!match) {
                    return false;
                }
            }

            return true;// 走到这一步，代表匹配成功
        } else {
            // 针对group by的情况，只要是一个包含关系，不需要顺序
            for (int i = 0; i < columns.size(); i++) {
                ISelectable column = columns.get(i);
                if (i >= orderBys.size()) {
                    return true; // 代表前缀匹配成功
                }

                boolean match = false;
                for (IOrderBy order : orderBys) {
                    if (order.getColumn().equals(column)) {
                        match = true;
                        break;
                    }
                }

                if (!match) {
                    return false;
                }
            }

            return true;// 走到这一步，代表匹配成功
        }

    }

    private static void buildTableFilter(TableNode tableNode) {
        List<List<IFilter>> DNFNodes = FilterUtils.toDNFNodesArray(tableNode.getWhereFilter());
        if (DNFNodes.size() == 1) {
            // 即使索引没有选择主键，但是有的filter依旧会在s主键上进行，作为valueFilter，所以要把主键也传进去
            Map<FilterType, IFilter> filters = FilterSpliter.splitByIndex(DNFNodes.get(0), tableNode);
            tableNode.setKeyFilter(filters.get(FilterType.IndexQueryKeyFilter));
            tableNode.setResultFilter(filters.get(FilterType.ResultFilter));
            tableNode.setIndexQueryValueFilter(filters.get(FilterType.IndexQueryValueFilter));
        } else {
            // 如果存在多个合取方式的的or组合时，无法区分出key/indexValue/result，直接下推
            tableNode.setResultFilter(tableNode.getWhereFilter());
        }
    }

    private static boolean isOptimizeJoinOrder(Map<String, Object> extraCmd) {
        return GeneralUtil.getExtraCmdBoolean(extraCmd, ExtraCmd.CHOOSE_JOIN, false);
    }
}
