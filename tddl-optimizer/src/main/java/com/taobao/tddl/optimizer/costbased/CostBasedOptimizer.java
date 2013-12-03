package com.taobao.tddl.optimizer.costbased;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.model.ExtraCmd;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.optimizer.Optimizer;
import com.taobao.tddl.optimizer.OptimizerContext;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.ast.DMLNode;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode.FilterType;
import com.taobao.tddl.optimizer.core.ast.dml.DeleteNode;
import com.taobao.tddl.optimizer.core.ast.dml.InsertNode;
import com.taobao.tddl.optimizer.core.ast.dml.PutNode;
import com.taobao.tddl.optimizer.core.ast.dml.UpdateNode;
import com.taobao.tddl.optimizer.core.ast.query.JoinNode;
import com.taobao.tddl.optimizer.core.ast.query.MergeNode;
import com.taobao.tddl.optimizer.core.ast.query.QueryNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.ast.query.strategy.BlockNestedLoopJoin;
import com.taobao.tddl.optimizer.core.ast.query.strategy.IndexNestedLoopJoin;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.ILogicalFilter;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.costbased.after.ChooseTreadOptimizer;
import com.taobao.tddl.optimizer.costbased.after.FillRequestIDAndSubRequestID;
import com.taobao.tddl.optimizer.costbased.after.FuckAvgOptimizer;
import com.taobao.tddl.optimizer.costbased.after.LimitOptimizer;
import com.taobao.tddl.optimizer.costbased.after.MergeConcurrentOptimizer;
import com.taobao.tddl.optimizer.costbased.after.MergeJoinMergeOptimizer;
import com.taobao.tddl.optimizer.costbased.after.RelationQueryOptimizer;
import com.taobao.tddl.optimizer.costbased.after.StreamingOptimizer;
import com.taobao.tddl.optimizer.costbased.before.BeforeOptimizerWalker;
import com.taobao.tddl.optimizer.costbased.before.QueryPlanOptimizer;
import com.taobao.tddl.optimizer.costbased.before.RemoveConstFilterProcessor;
import com.taobao.tddl.optimizer.costbased.before.TypeConvertProcessor;
import com.taobao.tddl.optimizer.costbased.esitimater.Cost;
import com.taobao.tddl.optimizer.costbased.esitimater.CostEsitimaterFactory;
import com.taobao.tddl.optimizer.exceptions.EmptyResultFilterException;
import com.taobao.tddl.optimizer.exceptions.QueryException;
import com.taobao.tddl.optimizer.exceptions.SqlParserException;
import com.taobao.tddl.optimizer.parse.SqlAnalysisResult;
import com.taobao.tddl.optimizer.parse.SqlParseManager;
import com.taobao.tddl.optimizer.parse.cobar.CobarSqlParseManager;
import com.taobao.tddl.optimizer.utils.FilterUtils;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * <pre>
 * 此优化器是根据开销进行优化的，主要优化流程在public IQueryCommon optimize(QueryTreeNode qn)中 
 * 分为两部分进行
 * a. 第一部分，对关系查询树的优化，包含以下几个步骤： 
 *  s1.将SELECT提前，放到叶子节点进行 SELECT列提前进行可以减少数据量
 *      由于一些列是作为连接列的，他们不在最后的SELECT中
 *          (比如SELECT table1.id from table1 join table2 on table1.name=table2.name table1.name和table2.name作为连接列)
 *      在对table1与table2的查询中应该保存，同时在执行执行结束后，需要将table1.name与table2.name去除.
 *      所以在执行这一步的时候，需要保存中间需要的临时列. 在生成执行计划后，需要将这些列从最后的节点中删除。 
 *  效果是：
 *      原SQL：table1.join(table2).addJoinColumns("id","id").select("table1.id table2.id")
 *      转换为：table1.select("table1.id").join(tabl2.select("table2.id")).addJoinColumns("id","id")
 *              
 *  s2.将Join中连接列上的约束条件复制到另一边 
 *      比如SELECT * from table1 join table2 on table1.id=table2.id where table1.id = 1
 *      因为Join是在table1.id与table2.id上的，所以table2.id上同样存在约束table2.id=1,此步就是需要发现这些条件，并将它复制。
 *  效果是：
 *      原SQL: table1.query("id=1").join(table2).addJoinColumns("id","id")
 *      转换为：table1.query("id=1").join(table2.query("id=2")).addJoinColumns("id","id")
 * 
 * s3.将约束条件提前，约束条件提前进行可以减少结果集的行数，并且可以合并QueryNode 
 *   效果是：
 *       原SQL:  table1.join(table2).addJoinColumns("id","id").query("table1.name=1")
 *       转换为: table1.query("table1.name=1").join(table2).addJoinColumns("id","id")
 * 
 * s4.找到并遍历每种个子查询，调整其Join顺序，并为其选择Join策略 
 * 
 * s5.所有子查询优化之后，再调整这个查询树的Join顺序
 *      对Join顺序调整的依据是通过计算开销，开销主要包括两种: 
 *          1. 磁盘IO与网络传输 详细计算方式请参见CostEstimater实现类的相关注释
 *          2. 对Join顺序的遍历使用的是最左树 在此步中，还会对同一列的约束条件进行合并等操作
 *      选取策略见chooseStrategyAndIndexAndSplitQuery的注释 
 * 
 * s6.将s1中生成的临时列删除
 * 
 * s7.将查询树转换为原始的执行计划树 
 * 
 * 第二部分，对执行计划树的优化，包含以下几个步骤： 
 * s8.为执行计划的每个节点选择执行的GroupNode
 *      这一步是根据TDDL的规则进行分库 在Join，Merge的执行节点选择上，遵循的原则是尽量减少网络传输 
 *  
 * s9.调整分库后的Join节点
 *      由于分库后，一个Query节点可能会变成一个Merge节点，需要对包含这样子节点的Join节点进行调整，详细见splitJoinAfterChooseDataNode的注释
 * </pre>
 * 
 * @since 5.1.0
 */
public class CostBasedOptimizer extends AbstractLifecycle implements Optimizer {

    private static final Logger           logger           = LoggerFactory.getLogger(CostBasedOptimizer.class);
    private int                           cacheSize        = 1000;
    private int                           expireTime       = 30000;
    private SqlParseManager               sqlParseManager;
    private Cache<String, OptimizeResult> optimizedResults;
    private List<RelationQueryOptimizer>  beforeOptimizers = new ArrayList<RelationQueryOptimizer>();
    private List<QueryPlanOptimizer>      afterOptimizers  = new ArrayList<QueryPlanOptimizer>();

    protected void doInit() {
        // before处理
        BeforeOptimizerWalker walker = new BeforeOptimizerWalker();
        walker.add(new RemoveConstFilterProcessor());
        walker.add(new TypeConvertProcessor());
        beforeOptimizers.add(walker);

        // after处理
        afterOptimizers.add(new FuckAvgOptimizer());
        afterOptimizers.add(new ChooseTreadOptimizer());
        afterOptimizers.add(new FillRequestIDAndSubRequestID());
        afterOptimizers.add(new LimitOptimizer());
        afterOptimizers.add(new MergeJoinMergeOptimizer());
        afterOptimizers.add(new MergeConcurrentOptimizer());
        afterOptimizers.add(new StreamingOptimizer());

        if (this.sqlParseManager == null) {
            CobarSqlParseManager sqlParseManager = new CobarSqlParseManager();
            sqlParseManager.setCacheSize(cacheSize);
            sqlParseManager.setExpireTime(expireTime);
            this.sqlParseManager = sqlParseManager;
        }

        if (!sqlParseManager.isInited()) {
            sqlParseManager.init(); // 启动
        }

        optimizedResults = CacheBuilder.newBuilder()
            .maximumSize(1000)
            .expireAfterWrite(30000, TimeUnit.MILLISECONDS)
            .build();
    }

    protected void doDestory() {
        optimizedResults.invalidateAll();
        sqlParseManager.destory();
    }

    private class OptimizeResult {

        public ASTNode        optimized = null;
        public QueryException ex        = null;
    }

    public IDataNodeExecutor optimizeAndAssignment(final ASTNode node,
                                                   final Map<Integer, ParameterContext> parameterSettings,
                                                   final Map<String, Comparable> extraCmd) throws QueryException {
        return optimizeAndAssignment(node, parameterSettings, extraCmd, null, false);
    }

    public IDataNodeExecutor optimizeAndAssignment(final String sql,
                                                   final Map<Integer, ParameterContext> parameterSettings,
                                                   final Map<String, Comparable> extraCmd, boolean cached)
                                                                                                          throws QueryException,
                                                                                                          SqlParserException {
        SqlAnalysisResult result = sqlParseManager.parse(sql, cached);
        return optimizeAndAssignment(result.getAstNode(parameterSettings), parameterSettings, extraCmd, sql, false);
    }

    private IDataNodeExecutor optimizeAndAssignment(final ASTNode node,
                                                    final Map<Integer, ParameterContext> parameterSettings,
                                                    final Map<String, Comparable> extraCmd, String sql, boolean cached)
                                                                                                                       throws QueryException {
        if (node.getSql() != null) { // 如果指定了sql，则绕过优化器直接返回
            if (logger.isDebugEnabled()) {
                logger.warn("node.getSql() != null:\n" + node.getSql());
            }

            node.executeOn(OptimizerContext.getContext().getRule().getDefaultGroup());
            return node.toDataNodeExecutor();
        }

        // long time = System.currentTimeMillis();
        ASTNode optimized = null;
        if (cached && sql != null && !sql.isEmpty()) {
            OptimizeResult or;
            try {
                or = optimizedResults.get(sql, new Callable<OptimizeResult>() {

                    public OptimizeResult call() throws Exception {
                        OptimizeResult or = new OptimizeResult();
                        try {
                            or.optimized = optimize(node, parameterSettings, extraCmd);
                        } catch (Exception e) {
                            if (e instanceof QueryException) {
                                or.ex = (QueryException) e;
                            } else {
                                or.ex = new QueryException(e);
                            }
                        }
                        return or;
                    }
                });
            } catch (ExecutionException e1) {
                throw new QueryException("Optimizer future task interrupted,the sql is:" + sql, e1);
            }

            if (or.ex != null) {
                throw or.ex;
            }
            optimized = or.optimized.deepCopy();
            optimized.build();
        } else {
            optimized = this.optimize(node, parameterSettings, extraCmd);
        }

        if (parameterSettings != null) {
            optimized.assignment(parameterSettings);
            this.mergeRestriction(optimized);
        }

        // 分库，选择执行节点
        try {
            optimized = DataNodeChooser.shard(optimized, parameterSettings, extraCmd);
        } catch (Exception e) {
            if (e instanceof QueryException) {
                throw (QueryException) e;
            } else {
                throw new QueryException(e);
            }
        }

        optimized = this.createMergeForJoin(optimized, parameterSettings, extraCmd);
        optimized = this.optimizeDistinct(optimized);
        this.pushOrderBy(optimized);

        IDataNodeExecutor qc = optimized.toDataNodeExecutor();
        // 进行一些自定义的额外处理
        for (QueryPlanOptimizer after : afterOptimizers) {
            qc = after.optimize(qc, parameterSettings, extraCmd);
        }
        if (logger.isDebugEnabled()) {
            logger.warn(qc.toString());
        }

        // time = System.currentTimeMillis();
        return qc;
    }

    public ASTNode optimize(ASTNode node, Map<Integer, ParameterContext> parameterSettings,
                            Map<String, Comparable> extraCmd) throws QueryException {
        node.build();
        for (RelationQueryOptimizer before : beforeOptimizers) {
            node = before.optimize(node, parameterSettings, extraCmd);
        }

        ASTNode optimized = null;
        if (node instanceof QueryTreeNode) {
            optimized = this.optimizeQuery((QueryTreeNode) node, parameterSettings, extraCmd);
        }

        if (node instanceof InsertNode) {
            optimized = this.optimizeInsert((InsertNode) node, parameterSettings, extraCmd);
        }

        else if (node instanceof DeleteNode) {
            optimized = this.optimizeDelete((DeleteNode) node, parameterSettings, extraCmd);
        }

        else if (node instanceof UpdateNode) {
            optimized = this.optimizeUpdate((UpdateNode) node, parameterSettings, extraCmd);
        }

        else if (node instanceof PutNode) {
            optimized = this.optimizePut((PutNode) node, parameterSettings, extraCmd);
        }

        return optimized;
    }

    private QueryTreeNode optimizeQuery(QueryTreeNode qn, Map<Integer, ParameterContext> parameterSettings,
                                        Map<String, Comparable> extraCmd) throws QueryException {

        findAndChangeRightJoinToLeftJoin(qn);

        // 将约束条件推向叶节点
        qn = this.pushRestriction(qn, null);

        qn = this.pushJoinOnFilter(qn, null);

        // 将Join中连接列上的约束条件复制到另一边
        this.copyFilterToJoinOnColumns(qn);

        // 将约束条件推向叶节点
        qn = this.pushRestriction(qn, null);
        // System.out.println(qn);

        // 将select提前
        pushSelect(qn);

        // 合并约束条件
        this.mergeRestriction(qn);

        // 重新build
        qn.build();

        // 找到每一个子查询，并进行优化
        Map<QueryTreeNode, IQueryTree> queryTreeRootAndItsPlan = new HashMap<QueryTreeNode, IQueryTree>();
        findAndOptimizeEverySubQuery(qn, queryTreeRootAndItsPlan, extraCmd);
        // 当每个子查询都优化完成后，优化整个查询
        qn = this.chooseJoinOrderAndIndexForASubQueryTree(qn, queryTreeRootAndItsPlan, extraCmd);
        return qn;
    }

    private ASTNode optimizeUpdate(UpdateNode update, Map<Integer, ParameterContext> parameterSettings,
                                   Map<String, Comparable> extraCmd) throws QueryException {
        update.build();
        if (extraCmd == null) extraCmd = new HashMap();
        // update暂不允许使用索引
        extraCmd.put(ExtraCmd.OptimizerExtraCmd.ChooseIndex, "FALSE");
        QueryTreeNode queryCommon = this.optimizeQuery(update.getNode(), parameterSettings, extraCmd);
        queryCommon.build();
        update.setNode((TableNode) queryCommon);
        return update;

    }

    private ASTNode optimizeInsert(InsertNode insert, Map<Integer, ParameterContext> parameterSettings,
                                   Map<String, Comparable> extraCmd) throws QueryException {
        insert.setNode((TableNode) insert.getNode().convertToJoinIfNeed());
        return insert;
    }

    private ASTNode optimizeDelete(DeleteNode delete, Map<Integer, ParameterContext> parameterSettings,
                                   Map<String, Comparable> extraCmd) throws QueryException {
        QueryTreeNode queryCommon = this.optimizeQuery(delete.getNode(), parameterSettings, extraCmd);
        delete.setNode((TableNode) queryCommon);
        return delete;
    }

    private ASTNode optimizePut(PutNode put, Map<Integer, ParameterContext> parameterSettings,
                                Map<String, Comparable> extraCmd) throws QueryException {
        return put;
    }

    // ============= helper method =============
    /**
     * 会遍历所有节点将right join的左右节点进行调换，转换成left join.
     * 
     * <pre>
     * 比如 A right join B on A.id = B.id
     * 转化为 B left join B on A.id = B.id
     * </pre>
     */
    private void findAndChangeRightJoinToLeftJoin(QueryTreeNode qtn) {
        for (ASTNode child : qtn.getChildren()) {
            this.findAndChangeRightJoinToLeftJoin((QueryTreeNode) child);
        }

        if (qtn instanceof JoinNode && ((JoinNode) qtn).isRightOuterJoin()) {
            /**
             * 如果带有其他非column=column条件，不能做这种转换，否则语义改变
             */
            if (qtn.getOtherJoinOnFilter() != null) {
                return;
            }

            JoinNode jn = (JoinNode) qtn;
            jn.exchangeLeftAndRight();
            jn.build();
        }
    }

    /**
     * 约束条件应该尽量提前
     * 
     * <pre>
     * 如： tabl1.join(table2).query("table1.id>10&&table2.id<5") 
     * 优化成: able1.query("table1.id>10").join(table2.query("table2.id<5")) t但如果条件中包含
     * 
     * ||条件则暂不优化
     * </pre>
     */
    private QueryTreeNode pushRestriction(QueryTreeNode qtn, List<IFilter> DNFNodeToPush) throws QueryException {
        // 如果是根节点，接收filter做为where条件,否则继续合并当前where条件，然后下推
        if (qtn.getChildren().isEmpty()) {
            IFilter node = FilterUtils.DNFNodeToBoolTree(DNFNodeToPush);
            if (node != null) {
                qtn.query(FilterUtils.and(qtn.getWhereFilter(), (IFilter) node.copy()));
                qtn.build();
            }
            return qtn;
        }

        // 对于 or连接的条件，就不能下推了
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
            QueryTreeNode child = this.pushRestriction(qn.getChild(), DNFNodeToPush);
            if (this.canIgnore(qn)) {
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
            }

            if (jn.isInnerJoin()) {
                jn.setLeftNode(this.pushRestriction(jn.getLeftNode(), DNFNodetoPushToLeft));
                jn.setRightNode(this.pushRestriction(((JoinNode) qtn).getRightNode(), DNFNodetoPushToRight));
            } else if (jn.isLeftOuterJoin()) {
                jn.setLeftNode(this.pushRestriction(jn.getLeftNode(), DNFNodetoPushToLeft));
                if (DNFNodeToPush != null && !DNFNodeToPush.isEmpty()) {
                    jn.query(FilterUtils.DNFNodeToBoolTree(DNFNodetoPushToRight)); // 在父节点完成filter，不能下推
                }
            } else if (jn.isRightOuterJoin()) {
                jn.setRightNode(this.pushRestriction(((JoinNode) qtn).getRightNode(), DNFNodetoPushToRight));
                if (DNFNodeToPush != null && !DNFNodeToPush.isEmpty()) {
                    jn.query(FilterUtils.DNFNodeToBoolTree(DNFNodetoPushToLeft));// 在父节点完成filter，不能下推
                }
            } else {
                if (DNFNodeToPush != null && !DNFNodeToPush.isEmpty()) {
                    jn.query(FilterUtils.DNFNodeToBoolTree(DNFNodeToPush));
                }
            }

            jn.build();
            return jn;

        } else {
            // TODO:MergeNode
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
    private QueryTreeNode pushJoinOnFilter(QueryTreeNode qtn, List<IFilter> DNFNodeToPush) throws QueryException {
        if (qtn.getChildren().isEmpty()) {
            IFilter node = FilterUtils.DNFNodeToBoolTree(DNFNodeToPush);
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
            QueryTreeNode child = this.pushJoinOnFilter(qn.getChild(), DNFNodeToPush);
            if (this.canIgnore(qn)) {
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
            }

            this.pushJoinOnFilter(jn.getLeftNode(), DNFNodetoPushToLeft);
            this.pushJoinOnFilter(jn.getRightNode(), DNFNodetoPushToRight);
            jn.build();
            return jn;
        } else {
            // TODO:MergeNode
        }
        return qtn;
    }

    /**
     * <pre>
     * 如果table1与table2 join table1.id = table2.id
     * 如果有约束table1.id = 1，那么同样应该有约束table2.id = 1，应该提前发现这一约束条件，来减小分库的范围
     * </pre>
     */
    private void copyFilterToJoinOnColumns(QueryTreeNode qtn) throws QueryException {
        if (qtn instanceof QueryNode) {
            QueryNode qn = (QueryNode) qtn;
            if (qn.getChild() != null) {
                this.copyFilterToJoinOnColumns(qn.getChild());
            }
        } else if (qtn instanceof JoinNode) {
            JoinNode j = (JoinNode) qtn;
            this.copyFilterToJoinOnColumns(j.getLeftNode());
            this.copyFilterToJoinOnColumns(j.getRightNode());

            QueryTreeNode left = j.getLeftNode();
            QueryTreeNode right = j.getRightNode();
            // 如果发现左边是QueryNode，就遍历左边的约束条件，如果发现有连接列的约束，则添加到一个列表里
            // 遍历完之后，使用pushRestriction将其推到右边的节点上,右边亦然
            if (left instanceof QueryNode) {
                this.copyFilterToJoinOnColumns((QueryNode) left, right, j.getLeftKeys(), j.getRightKeys());
            }
            if (right instanceof QueryNode) {
                this.copyFilterToJoinOnColumns((QueryNode) right, left, j.getRightKeys(), j.getLeftKeys());
            }
        } else if (qtn instanceof MergeNode) {
            MergeNode m = (MergeNode) qtn;
            for (int i = 0; i < m.getChildren().size(); i++) {
                this.copyFilterToJoinOnColumns((QueryTreeNode) m.getChildren().get(i));
            }

        } else {
            // 其他情况不处理，退出递归
        }

    }

    /**
     * 遍历源节点的约束条件 将连接列上的约束复制到目标节点内
     * 
     * @param qn 要复制的源节点
     * @param other 要复制的目标节点
     * @param qnColumns 源节点的join字段
     * @param otherColumns 目标节点的join字段
     * @throws QueryException
     */
    private void copyFilterToJoinOnColumns(QueryNode qn, QueryTreeNode other, List<ISelectable> qnColumns,
                                           List<ISelectable> otherColumns) throws QueryException {
        List<List<IFilter>> srcDNFNodes = FilterUtils.toDNFNodesArray(qn.getWhereFilter());
        // 如果join包含or条件，暂不支持处理，不然语义不对
        if (srcDNFNodes.size() > 1 || srcDNFNodes.isEmpty()) {
            return;
        }

        List<IFilter> DNF = srcDNFNodes.get(0);
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

        this.pushRestriction(other, newIFilterToPush);
    }

    /**
     * 将select查询push到叶子节点，需要从最底层将数据带上来
     * 
     * @param qn
     * @throws QueryException
     */
    private void pushSelect(QueryTreeNode qn) throws QueryException {
        if (qn instanceof QueryNode) {
            QueryTreeNode child = ((QueryNode) qn).getChild();
            if (child == null || child.isSubQuery()) {
                return;
            } else {
                List<ISelectable> columnsToPush = new LinkedList<ISelectable>();
                for (ISelectable c : qn.getColumnsRefered()) {
                    if (c instanceof IColumn) {
                        columnsToPush.add(c.copy().setAlias(null));
                    }
                }
                child.select(columnsToPush);
                child.build();
                pushSelect(child);
                return;
            }
        } else if (qn instanceof MergeNode) {
            // merge的push逻辑放到了MergeBuilder里
            throw new IllegalArgumentException("不支持Merge的push");
        } else if (qn instanceof JoinNode) {
            List<ISelectable> columns = new LinkedList<ISelectable>(qn.getColumnsRefered());
            for (ASTNode child : qn.getChildren()) {
                if (((QueryTreeNode) child).isSubQuery()) {
                    continue;
                }

                if (!columns.containsAll(((QueryTreeNode) child).getColumnsSelected())) {
                    List<ISelectable> newChildSelected = new ArrayList<ISelectable>(columns.size());
                    for (ISelectable c : columns) {
                        boolean flag = false;
                        if (c instanceof IFunction) {
                            continue;
                        }

                        if (((QueryTreeNode) child).getColumnsSelectedForParent().contains(c)) {
                            flag = true;
                        }

                        if (flag) {
                            newChildSelected.add(c.copy().setAlias(null));
                        }
                    }

                    ((QueryTreeNode) child).select(newChildSelected);
                    child.build();
                }
            }
        }
    }

    /**
     * 将merge/join中的order by条件下推，包括隐式的order by条件，比如将groupBy转化为orderBy
     * 
     * <pre>
     * 比如: tabl1.join(table2).on("table1.id=table2.id").orderBy("id")
     * 转化为：table.orderBy(id).join(table2).on("table1.id=table2.id")
     * 
     * </pre>
     */
    private void pushOrderBy(ASTNode qtn) {
        if (qtn instanceof MergeNode) {
            MergeNode merge = (MergeNode) qtn;

            if (!(merge.getChild() instanceof QueryTreeNode)) {
                return;
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
        }

        // index nested loop中的order by，可以推到左节点
        if (qtn instanceof JoinNode) {
            JoinNode join = (JoinNode) qtn;
            if (join.getJoinStrategy() instanceof IndexNestedLoopJoin) {
                if (join.getImplicitOrderBys() != null && !join.getImplicitOrderBys().isEmpty()) {
                    List<IOrderBy> orders = join.getImplicitOrderBys();
                    for (IOrderBy order : orders) {
                        if (join.getLeftNode().hasColumn(order.getColumn())) {
                            join.getLeftNode().orderBy(order.getColumn().copy(), order.getDirection());
                        } else if (join.isUedForIndexJoinPK()) {
                            ISelectable newC = order.getColumn().copy().setTableName(null);
                            if (join.getLeftNode().hasColumn(newC)) {
                                join.getLeftNode().orderBy(newC, order.getDirection());
                            }
                        }
                    }
                    join.getLeftNode().build();
                }
            }

        }

        if (!(qtn instanceof QueryTreeNode)) {
            return;
        }

        for (ASTNode child : ((QueryTreeNode) qtn).getChildren()) {
            if (child instanceof QueryTreeNode) {
                this.pushOrderBy((QueryTreeNode) child);
            }
        }
    }

    private ASTNode createMergeForJoin(ASTNode dne, Map<Integer, ParameterContext> parameterSettings,
                                       Map<String, Comparable> extraCmd) {
        if (dne instanceof MergeNode) {
            for (ASTNode sub : ((MergeNode) dne).getChildren()) {
                this.createMergeForJoin(sub, parameterSettings, extraCmd);
            }
        }

        if (dne instanceof JoinNode) {
            this.createMergeForJoin(((JoinNode) dne).getLeftNode(), parameterSettings, extraCmd);
            this.createMergeForJoin(((JoinNode) dne).getRightNode(), parameterSettings, extraCmd);

            if (((JoinNode) dne).getRightNode() instanceof QueryNode) {
                QueryNode right = (QueryNode) ((JoinNode) dne).getRightNode();

                if (right.getDataNode() != null) {
                    // right和join节点跨机，则需要右边生成Merge来做mget
                    if (!right.getDataNode().equals(dne.getDataNode())) {
                        MergeNode merge = new MergeNode();
                        merge.merge(right);
                        merge.setSharded(false);
                        merge.executeOn(right.getDataNode());
                        merge.build();
                        ((JoinNode) dne).setRightNode(merge);
                    }
                }
            }
        }

        if (dne instanceof QueryNode) {
            if (((QueryNode) dne).getChild() != null) {
                this.createMergeForJoin(((QueryNode) dne).getChild(), parameterSettings, extraCmd);
            }
        }

        return dne;
    }

    /**
     * 处理下distinct函数，将merge的子节点的function函数删除，由上层来计算
     */
    private ASTNode optimizeDistinct(ASTNode qtn) {
        if (!(qtn instanceof QueryTreeNode)) {
            return qtn;
        }

        for (ASTNode child : ((QueryTreeNode) qtn).getChildren()) {
            if (child instanceof QueryTreeNode) {
                this.optimizeDistinct((QueryTreeNode) child);
            }
        }

        if (qtn instanceof MergeNode) {
            MergeNode merge = (MergeNode) qtn;
            if (!(merge.getChild() instanceof QueryTreeNode)) {
                return merge;
            }

            if (containsDistinctColumns(merge)) {
                // 将Merge及子节点中的聚合函数 group by都去掉
                for (ASTNode con : merge.getChildren()) {
                    QueryTreeNode child = (QueryTreeNode) con;
                    List<IFunction> toRemove = new ArrayList();
                    for (ISelectable s : child.getColumnsSelected()) {
                        if (s instanceof IFunction) {
                            toRemove.add((IFunction) s);
                        }
                    }
                    child.setOrderBys(new ArrayList(0));
                    child.getColumnsSelected().removeAll(toRemove);
                    child.build();

                    for (IOrderBy group : child.getGroupBys()) {
                        child.orderBy(group.getColumn(), group.getDirection());
                    }
                    // distinct group by同时存在时，要先安group by的列排序
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

    private boolean containsDistinctColumns(QueryTreeNode qc) {
        for (ISelectable c : qc.getColumnsSelected()) {
            if (isDistinct(c)) {
                return true;
            }
        }
        return false;
    }

    private boolean isDistinct(ISelectable s) {
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
     * 将filter中的and/or条件中进行Range合并处理 <br/>
     * 
     * <pre>
     * 比如: 
     *  a. A =1 And A =2 ，永远false条件，返回EmptyResultFilterException异常
     *  b. (1 < A < 5) or (2 < A < 6)，合并为 (1 < A < 6)
     *  c. A <= 1 or A = 1，永远true条件
     * </pre>
     */
    private void mergeRestriction(ASTNode on) throws EmptyResultFilterException {
        if (on == null) {
            return;
        }

        if (on instanceof QueryTreeNode) {
            QueryTreeNode node = (QueryTreeNode) on;
            for (int i = 0; i < node.getChildren().size(); i++) {
                QueryTreeNode child = (QueryTreeNode) node.getChildren().get(i);
                this.mergeRestriction(child);
            }

            node.query(FilterUtils.merge(node.getWhereFilter()));
            node.setKeyFilter(FilterUtils.merge(node.getKeyFilter()));
            node.setResultFilter(FilterUtils.merge(node.getResultFilter()));
            if (node instanceof TableNode) {
                ((TableNode) node).setIndexQueryValueFilter(FilterUtils.merge(((TableNode) node).getIndexQueryValueFilter()));
            }
        } else if (on instanceof DMLNode) {
            this.mergeRestriction(((DMLNode) on).getNode());
        } else {
            // 也不会有其他情况
        }
    }

    /**
     * <pre>
     * 由于优化过程中需要将QueryTree转换为执行计划树
     * 而外部查询转换为执行计划树是依赖于子查询的
     * 所以需要先对子查询进行优化，再对外层查询进行优化
     * 回溯完成
     * </pre>
     */
    private void findAndOptimizeEverySubQuery(QueryTreeNode qtn,
                                              Map<QueryTreeNode, IQueryTree> queryTreeRootAndItsPlan,
                                              Map<String, Comparable> extraCmd) throws QueryException {
        if (qtn instanceof QueryNode) {
            QueryNode qn = (QueryNode) qtn;
            if (qn.getChild() != null) {
                findAndOptimizeEverySubQuery(qn.getChild(), queryTreeRootAndItsPlan, extraCmd);
                qn.setChild(chooseJoinOrderAndIndexForASubQueryTree(qn.getChild(), queryTreeRootAndItsPlan, extraCmd));
            }
        } else if (qtn instanceof JoinNode) {
            JoinNode jn = (JoinNode) qtn;
            this.findAndOptimizeEverySubQuery(jn.getLeftNode(), queryTreeRootAndItsPlan, extraCmd);
            this.findAndOptimizeEverySubQuery(jn.getRightNode(), queryTreeRootAndItsPlan, extraCmd);
        } else {// table/merge
            if ((!qtn.getChildren().isEmpty()) && qtn.getChildren().get(0) != null) {
                findAndOptimizeEverySubQuery((QueryTreeNode) qtn.getChildren().get(0),
                    queryTreeRootAndItsPlan,
                    extraCmd);
            }
        }

    }

    /**
     * 优化一棵查询树,以QueryNode为叶子终止,QueryNode的子节点不处理。
     */
    private QueryTreeNode chooseJoinOrderAndIndexForASubQueryTree(QueryTreeNode qn,
                                                                  Map<QueryTreeNode, IQueryTree> queryTreeRootAndItsPlan,
                                                                  Map<String, Comparable> extraCmd)
                                                                                                   throws QueryException {
        // 暂时跳过可能存在的聚合函数
        if (!(qn instanceof TableNode || qn instanceof JoinNode)) {
            qn.getChildren().set(0,
                chooseJoinOrderAndIndexForASubQueryTree((QueryTreeNode) qn.getChildren().get(0),
                    queryTreeRootAndItsPlan,
                    extraCmd));

            return qn;
        }

        JoinPermutationGenerator jpg = null;
        if (this.isOptimizeJoinOrder()) {
            jpg = new JoinPermutationGenerator(qn);
            qn = jpg.getNext();
        }

        long minIo = Long.MAX_VALUE;
        QueryTreeNode minCostQueryTree = null;

        // 枚举每一种join的顺序，并计算开销，每次保留当前开销最小的Join次序
        while (qn != null) {
            qn = chooseStrategyAndIndexAndSplitQuery(qn, extraCmd);
            qn = qn.convertToJoinIfNeed();
            this.pushOrderBy(qn);

            if (this.isOptimizeJoinOrder()) {
                Cost cost = CostEsitimaterFactory.estimate(qn);
                if (cost.getScanCount() < minIo) {
                    minIo = cost.getScanCount();
                    minCostQueryTree = qn;
                }
                qn = jpg.getNext();
            } else {
                minCostQueryTree = qn;
                break;
            }

        }

        minCostQueryTree.build();
        return minCostQueryTree;
    }

    /**
     * 遍历每个节点 分解Query 为Query选择索引与Join策略 只遍历一棵子查询树
     */
    private QueryTreeNode chooseStrategyAndIndexAndSplitQuery(QueryTreeNode node, Map<String, Comparable> extraCmd)
                                                                                                                   throws QueryException {
        if (node instanceof JoinNode) {
            for (int i = 0; i < node.getChildren().size(); i++) {
                QueryTreeNode child = (QueryTreeNode) node.getChildren().get(i);
                child = this.chooseStrategyAndIndexAndSplitQuery(child, extraCmd);
                node.getChildren().set(i, child);
            }
        }

        if (node instanceof TableNode) {
            // Query是对实体表进行查询
            List<QueryTreeNode> ss = FilterSpliter.split((TableNode) node, extraCmd);

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
        } else if (node instanceof JoinNode) {
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
                    IndexMeta index = IndexChooser.findBestIndex((((TableNode) innerNode).getIndexs()),
                        ((JoinNode) node).getRightKeys(),
                        Collections.<IFilter> emptyList(),
                        tablename,
                        extraCmd);
                    ((TableNode) innerNode).useIndex(index);
                    List<List<IFilter>> DNFNodes = FilterUtils.toDNFNodesArray(innerNode.getWhereFilter());
                    if (!DNFNodes.isEmpty()) {
                        if (DNFNodes.size() != 1) {// 多个or条件
                            throw new IllegalAccessError();
                        }
                        // 即使索引没有选择主键，但是有的filter依旧会在s主键上进行，作为valueFilter，所以要把主键也传进去
                        Map<FilterType, IFilter> filters = FilterSpliter.splitByType(DNFNodes.get(0),
                            (TableNode) innerNode);

                        ((TableNode) innerNode).setKeyFilter(filters.get(FilterType.IndexQueryKeyFilter));
                        ((TableNode) innerNode).setResultFilter(filters.get(FilterType.ResultFilter));
                        ((TableNode) innerNode).setIndexQueryValueFilter(filters.get(FilterType.IndexQueryValueFilter));
                    }

                    if (index == null) {// case 1.1
                        ((JoinNode) node).setJoinStrategy(new IndexNestedLoopJoin());
                    } else {// case 1.2
                        for (IBooleanFilter filter : ((JoinNode) node).getJoinFilter()) {
                            ISelectable rightColumn = (ISelectable) filter.getValue();
                            boolean isInIndex = false;
                            for (ColumnMeta cm : index.getKeyColumns()) {
                                if (rightColumn.getColumnName().equals(cm.getName())) {
                                    isInIndex = true;
                                    break;
                                }
                            }

                            if (!isInIndex) {
                                node.addResultFilter(filter);
                            }

                        }

                        // 删除join条件中的非索引的列，将其做为where条件
                        if (node.getWhereFilter() != null) {
                            if (node.getWhereFilter() instanceof IBooleanFilter) {
                                ((JoinNode) node).getJoinFilter().remove(node.getWhereFilter());
                            } else {
                                ((JoinNode) node).getJoinFilter()
                                    .removeAll(((ILogicalFilter) ((JoinNode) node).getWhereFilter()).getSubFilter());
                            }
                            node.build();
                        }
                        ((JoinNode) node).setJoinStrategy(new IndexNestedLoopJoin());
                    }

                    ((TableNode) innerNode).useIndex(index);

                } else {// case 2，因为2.1与2.2现在使用同一种策略，就是使用NestLoop...
                    ((JoinNode) node).setJoinStrategy(new IndexNestedLoopJoin());
                }

            } else { // 这种情况也属于case 2，先使用NestLoop...
                ((JoinNode) node).setJoinStrategy(new IndexNestedLoopJoin());
            }

            return node;
        } else if (node instanceof MergeNode) {
            return node;
        } else {
            assert (false);
            return node;
        }
    }

    // 判断一个QueryNode是否可以被删掉
    private boolean canIgnore(QueryNode qn) {
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

    /**
     * 将原本的Join的where条件中的a.id=b.id构建为join条件，并从where条件中移除
     */
    private List<IFilter> addJoinKeysFromDNFNodeAndRemoveIt(List<IFilter> DNFNode, JoinNode join) throws QueryException {
        // filter中可能包含join列,如id=id
        // 目前必须满足以下条件
        // 1、不包含or
        // 2、=连接
        if (this.isFilterContainsColumnOnTheRight(DNFNode)) {
            List<Object> leftJoinKeys = new ArrayList<Object>();
            List<Object> rightJoinKeys = new ArrayList<Object>();

            List<IFilter> filtersToRemove = new LinkedList();
            for (IFilter sub : DNFNode) {
                // 如果join node的where条件中是个组合条件
                if (!(sub instanceof IBooleanFilter)) {
                    throw new IllegalArgumentException("查询条件操作符的右边包含列，并且过于复杂，暂无法解析，请修改查询语句...");
                }

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

    private boolean isFilterContainsColumnOnTheRight(List<IFilter> DNFNode) {
        for (IFilter f : DNFNode) {
            if (f instanceof IBooleanFilter) {
                if (((IBooleanFilter) f).getValue() instanceof IColumn) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 找到join的列条件的所有列信息，必须是a.id=b.id的情况，针对a.id=1返回为null
     */
    private ISelectable[] getJoinKeysWithColumnJoin(IBooleanFilter filter) {
        if (((IBooleanFilter) filter).getColumn() instanceof IColumn
            && ((IBooleanFilter) filter).getValue() instanceof IColumn) {
            if (OPERATION.EQ.equals(filter.getOperation())) {
                return new ISelectable[] { (ISelectable) ((IBooleanFilter) filter).getColumn(),
                        (ISelectable) ((IBooleanFilter) filter).getValue() };
            }
        }

        return null;
    }

    public boolean isOptimizeJoinOrder() {
        return true;
    }

    public void setCacheSize(int cacheSize) {
        this.cacheSize = cacheSize;
    }

    public void setExpireTime(int expireTime) {
        this.expireTime = expireTime;
    }

    public void setSqlParseManager(SqlParseManager sqlParseManager) {
        this.sqlParseManager = sqlParseManager;
    }

}
