package com.taobao.tddl.optimizer.costbased.chooser;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.BooleanUtils;

import com.taobao.tddl.common.model.ExtraCmd;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode.FilterType;
import com.taobao.tddl.optimizer.core.ast.query.JoinNode;
import com.taobao.tddl.optimizer.core.ast.query.MergeNode;
import com.taobao.tddl.optimizer.core.ast.query.QueryNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.ast.query.strategy.BlockNestedLoopJoin;
import com.taobao.tddl.optimizer.core.ast.query.strategy.IndexNestedLoopJoin;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.ILogicalFilter;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.costbased.esitimater.Cost;
import com.taobao.tddl.optimizer.costbased.esitimater.CostEsitimaterFactory;
import com.taobao.tddl.optimizer.costbased.pusher.OrderByPusher;
import com.taobao.tddl.optimizer.exceptions.QueryException;
import com.taobao.tddl.optimizer.utils.FilterUtils;

/**
 * 优化一下join策略
 */
public class JoinChooser {

    /**
     * @param qtn
     * @param extraCmd
     * @return
     */
    public static QueryTreeNode optimize(QueryTreeNode qtn, Map<String, Comparable> extraCmd) {
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
    public static void optimizeSubQuery(QueryTreeNode qtn, Map<String, Comparable> extraCmd) throws QueryException {
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
    public static QueryTreeNode optimizeJoin(QueryTreeNode qtn, Map<String, Comparable> extraCmd) {
        // 暂时跳过可能存在的聚合函数
        if (!(qtn instanceof TableNode || qtn instanceof JoinNode)) {
            qtn.getChildren().set(0, optimize((QueryTreeNode) qtn.getChildren().get(0), extraCmd));
            return qtn;
        }

        JoinPermutationGenerator jpg = null;
        if (isOptimizeJoinOrder(extraCmd)) {
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
     * 遍历每个节点 分解Query 为Query选择索引与Join策略 只遍历一棵子查询树
     */
    private static QueryTreeNode chooseStrategyAndIndexAndSplitQuery(QueryTreeNode node,
                                                                     Map<String, Comparable> extraCmd)
                                                                                                      throws QueryException {
        if (node instanceof JoinNode) {
            for (int i = 0; i < node.getChildren().size(); i++) {
                QueryTreeNode child = (QueryTreeNode) node.getChildren().get(i);
                child = chooseStrategyAndIndexAndSplitQuery(child, extraCmd);
                node.getChildren().set(i, child);
            }

            // 如果Join是OuterJoin，则不区分内外表，只能使用NestLoop或者SortMerge，暂时只使用SortMerge
            // if (((JoinNode) node).isOuterJoin()) {
            // ((JoinNode) node).setJoinStrategy(new BlockNestedLoopJoin(oc));
            //
            // return node;
            // }

            // 如果右表是subquery，则也不能用indexNestedLoop
            if (((JoinNode) node).getRightNode().isSubQuery()) {
                ((JoinNode) node).setJoinStrategy(new BlockNestedLoopJoin());
                return node;
            }

            /**
             * <pre>
             * 如果内表是一个TableNode，或者是一个紧接着TableNode的QueryNode
             * 先只考虑约束条件，而不考虑Join条件分以下两种大类情况：
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
             *      这种情况最为复杂，有两种方法
             *          a. 放弃原来根据约束条件选择的索引，而使用Join列中得索引(如果有的话)，将约束条件全部作为ValueFilter，这样可以使用IndexNestLoop
             *          b. 采用根据约束条件选择的索引，而不管Join列，这样只能使用NestLoop
             *      如果内表经约束后的大小比较小，则可以使用方案二，反之，则应使用方案一,不过此开销目前很难估算。
             *      或者枚举所有可能的情况，貌似也比较麻烦，暂时只采用方案二，实现简单一些。
             * </pre>
             */

            QueryTreeNode innerNode = ((JoinNode) node).getRightNode();
            if (innerNode instanceof TableNode) {
                if (!((TableNode) innerNode).containsKeyFilter()) {
                    String tablename = ((TableNode) innerNode).getTableName();
                    if (innerNode.getAlias() != null) {
                        tablename = innerNode.getAlias();
                    }
                    // 找到对应join字段的索引
                    IndexMeta index = IndexChooser.findBestIndex((((TableNode) innerNode).getIndexs()),
                        ((JoinNode) node).getRightKeys(),
                        Collections.<IFilter> emptyList(),
                        tablename,
                        extraCmd);
                    ((TableNode) innerNode).useIndex(index);
                    List<List<IFilter>> DNFNodes = FilterUtils.toDNFNodesArray(innerNode.getWhereFilter());
                    if (DNFNodes.size() == 1) {
                        // 即使索引没有选择主键，但是有的filter依旧会在s主键上进行，作为valueFilter，所以要把主键也传进去
                        Map<FilterType, IFilter> filters = FilterChooser.optimize(DNFNodes.get(0),
                            (TableNode) innerNode);
                        ((TableNode) innerNode).setKeyFilter(filters.get(FilterType.IndexQueryKeyFilter));
                        ((TableNode) innerNode).setResultFilter(filters.get(FilterType.ResultFilter));
                        ((TableNode) innerNode).setIndexQueryValueFilter(filters.get(FilterType.IndexQueryValueFilter));
                    } else {
                        // 如果存在多个合取方式的的or组合时，无法区分出key/indexValue/result，直接下推
                        ((TableNode) innerNode).setResultFilter(innerNode.getWhereFilter());
                    }

                    if (index == null) {// case 1.1
                        ((JoinNode) node).setJoinStrategy(new IndexNestedLoopJoin());
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

                        ((JoinNode) node).setJoinStrategy(new IndexNestedLoopJoin());
                    }
                } else {// case 2，因为2.1与2.2现在使用同一种策略，就是使用NestLoop...
                    ((JoinNode) node).setJoinStrategy(new IndexNestedLoopJoin());
                }

            } else { // 这种情况也属于case 2，先使用NestLoop...
                ((JoinNode) node).setJoinStrategy(new IndexNestedLoopJoin());
            }

            return node;
        } else if (node instanceof TableNode) {
            // Query是对实体表进行查询
            List<QueryTreeNode> ss = FilterChooser.optimize((TableNode) node, extraCmd);
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
                ((TableNode) node).setIndexQueryValueFilter(node.getWhereFilter());
                ((TableNode) node).setKeyFilter(null);
                node.setResultFilter(null);
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
            }

            // 全表扫描可以扫描包含所有选择列的索引
            IndexMeta indexWithAllColumnsSelected = IndexChooser.findIndexWithAllColumnsSelected(((TableNode) node).getIndexs(),
                (TableNode) node);

            if (indexWithAllColumnsSelected != null) {
                ((TableNode) node).useIndex(indexWithAllColumnsSelected);
            }
            return node;
        } else {
            return node;
        }
    }

    private static boolean isOptimizeJoinOrder(Map<String, Comparable> extraCmd) {
        return BooleanUtils.toBoolean(extraCmd.get(ExtraCmd.OptimizerExtraCmd.ChooseJoin).toString());
    }
}
