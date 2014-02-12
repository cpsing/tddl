package com.taobao.tddl.optimizer.costbased;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.taobao.tddl.common.model.ExtraCmd;
import com.taobao.tddl.optimizer.BaseOptimizerTest;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.query.JoinNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;
import com.taobao.tddl.optimizer.core.plan.query.IJoin.JoinStrategy;
import com.taobao.tddl.optimizer.core.plan.query.IMerge;
import com.taobao.tddl.optimizer.core.plan.query.IParallelizableQueryTree.QUERY_CONCURRENCY;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;
import com.taobao.tddl.optimizer.utils.OptimizerUtils;

/**
 * 整个优化器的集成测试，主要是一些query
 */
public class OptimizerTest extends BaseOptimizerTest {

    private static Map<String, Object> extraCmd = new HashMap<String, Object>();

    @BeforeClass
    public static void setUp() {
        extraCmd.put(ExtraCmd.CHOOSE_INDEX, true);
        extraCmd.put(ExtraCmd.CHOOSE_JOIN, false);
        extraCmd.put(ExtraCmd.CHOOSE_INDEX_MERGE, false);
        extraCmd.put(ExtraCmd.MERGE_EXPAND, false);
        extraCmd.put(ExtraCmd.JOIN_MERGE_JOIN_JUDGE_BY_RULE, true);
    }

    @Test
    public void test_单表查询_无条件() {
        TableNode table = new TableNode("TABLE1");
        QueryTreeNode qn = table;
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(qn, null, extraCmd);

        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertEquals(QUERY_CONCURRENCY.SEQUENTIAL, ((IMerge) qc).getQueryConcurrency());// 串行
        IDataNodeExecutor dne = ((IMerge) qc).getSubNode().get(0);
        Assert.assertTrue(dne instanceof IQuery);
        IQuery query = (IQuery) dne;
        Assert.assertEquals(null, query.getKeyFilter());
        Assert.assertEquals(null, query.getValueFilter());
    }

    // 单表主键查询
    // ID为主键，同时在ID上存在索引
    // 直接查询KV ID->data
    // keyFilter为ID=1
    @Test
    public void test_单表查询_主键条件() {
        TableNode table = new TableNode("TABLE1");
        table.query("ID=1 AND ID<40");

        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(table, null, extraCmd);
        Assert.assertTrue(qc instanceof IQuery);
        Assert.assertEquals("TABLE1.ID = 1", ((IQuery) qc).getKeyFilter().toString());
        Assert.assertEquals(null, ((IQuery) qc).getValueFilter());
    }

    // 单表主键查询
    // ID为主键，同时在ID上存在索引
    // 因为!=不能使用主键索引
    // valueFilter为ID!=1
    @Test
    public void test_单表查询_主键条件_不等于只能是valueFilter() {
        TableNode table = new TableNode("TABLE1");
        table.query("ID != 1");
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(table, null, extraCmd);

        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertEquals(QUERY_CONCURRENCY.CONCURRENT, ((IMerge) qc).getQueryConcurrency());// 并行
        IDataNodeExecutor dne = ((IMerge) qc).getSubNode().get(0);
        Assert.assertTrue(dne instanceof IQuery);
        IQuery query = (IQuery) dne;
        Assert.assertEquals(null, query.getKeyFilter());
        Assert.assertEquals("TABLE1.ID != 1", query.getValueFilter().toString());
    }

    // 单表非主键索引查询
    // NAME上存在索引
    // 会生成一个Join节点
    // 左边通过NAME索引找到满足条件的PK，keyFilter应该为NAME=1
    // 与pk->data Join
    // Join类型为IndexNestLoop
    @Test
    public void test_单表查询_value条件() {
        TableNode table = new TableNode("TABLE1");
        table.query("NAME = 1");

        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(table, null, extraCmd);
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertEquals(QUERY_CONCURRENCY.CONCURRENT, ((IMerge) qc).getQueryConcurrency());// 并行
        IDataNodeExecutor dne = ((IMerge) qc).getSubNode().get(0);
        Assert.assertTrue(dne instanceof IJoin);
        IJoin join = (IJoin) dne;
        IQuery left = (IQuery) join.getLeftNode();
        Assert.assertEquals("TABLE1._NAME.NAME = 1", left.getKeyFilter().toString());
    }

    // 单表非主键无索引查询
    // SCHOOL上不存在索引
    // 所以会执行全表扫描
    // 只会生成一个IQuery
    // SCHOOL=1作为valueFilter
    @Test
    public void test_单表查询_非任何索引条件() {
        TableNode table = new TableNode("TABLE1");
        table.query("SCHOOL = 1");

        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(table, null, extraCmd);
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertEquals(QUERY_CONCURRENCY.CONCURRENT, ((IMerge) qc).getQueryConcurrency());// 并行
        IDataNodeExecutor dne = ((IMerge) qc).getSubNode().get(0);
        Assert.assertTrue(dne instanceof IQuery);
        IQuery query = (IQuery) dne;
        Assert.assertEquals("TABLE1.SCHOOL = 1", query.getValueFilter().toString());
    }

    // 单表or查询
    // 查询条件由or连接，
    // 由于NAME和ID上存在索引，所以会生成两个子查询
    // or的两边分别作为子查询的keyFilter
    // 由于NAME=2323的子查询为非主键索引查询
    // 所以此处会生成一个join节点
    // 最后一个merge节点用于合并子查询的结果
    @Test
    public void test_单表查询_OR条件_1() {
        TableNode table = new TableNode("TABLE1");
        table.query("NAME = 2323 OR ID=1");
        extraCmd.put(ExtraCmd.CHOOSE_INDEX_MERGE, true);
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(table, null, extraCmd);
        extraCmd.put(ExtraCmd.CHOOSE_INDEX_MERGE, false);
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertTrue(((IMerge) qc).isUnion());// 是union查询
        Assert.assertTrue(((IMerge) qc).getSubNode().get(0) instanceof IQuery);
        IQuery query = (IQuery) ((IMerge) qc).getSubNode().get(0);
        Assert.assertEquals("TABLE1.ID = 1", query.getKeyFilter().toString());
        Assert.assertTrue(((IMerge) qc).getSubNode().get(1) instanceof IMerge);
        Assert.assertTrue(((IMerge) ((IMerge) qc).getSubNode().get(1)).getSubNode().get(0) instanceof IJoin);
        IJoin join = (IJoin) ((IMerge) ((IMerge) qc).getSubNode().get(1)).getSubNode().get(0);
        Assert.assertEquals("TABLE1._NAME.NAME = 2323", ((IQuery) join.getLeftNode()).getKeyFilter().toString());
    }

    // 单表复杂查询条件
    // SCHOOL=1 AND (ID=4 OR ID=3)
    // 应该展开为
    // (SCHOOL=1 AND ID=4) OR (SCHOOL=1 AND ID=3)
    @Test
    public void test_单表查询_复杂条件展开() {
        TableNode table = new TableNode("TABLE1");
        table.query("SCHOOL=1 AND (ID=4 OR ID=3)");
        extraCmd.put(ExtraCmd.CHOOSE_INDEX_MERGE, true);
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(table, null, extraCmd);
        extraCmd.put(ExtraCmd.CHOOSE_INDEX_MERGE, false);
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertTrue(((IMerge) qc).isUnion());// 是union查询
        Assert.assertTrue(((IMerge) qc).getSubNode().get(0) instanceof IQuery);
        Assert.assertTrue(((IMerge) qc).getSubNode().get(1) instanceof IQuery);
        IQuery query1 = (IQuery) ((IMerge) qc).getSubNode().get(0);
        Assert.assertEquals("TABLE1.ID = 4", query1.getKeyFilter().toString());
        Assert.assertEquals("TABLE1.SCHOOL = 1", query1.getValueFilter().toString());
        IQuery query2 = (IQuery) ((IMerge) qc).getSubNode().get(1);
        Assert.assertEquals("TABLE1.ID = 3", query2.getKeyFilter().toString());
        Assert.assertEquals("TABLE1.SCHOOL = 1", query2.getValueFilter().toString());
    }

    // 两表Join查询，右表连接键为主键，右表为主键查询
    // 开启了join merge join
    // 右表为主键查询的情况下，Join策略应该选择IndexNestLoop
    @Test
    public void test_两表Join_主键() {
        TableNode table = new TableNode("TABLE1");
        JoinNode join = table.join("TABLE2", "ID", "ID");
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(join, null, extraCmd);
        // 应该是join merge join
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertTrue(((IMerge) qc).getSubNode().get(0) instanceof IJoin);
        IJoin subJoin = (IJoin) ((IMerge) qc).getSubNode().get(0);
        Assert.assertEquals(JoinStrategy.INDEX_NEST_LOOP, subJoin.getJoinStrategy());
    }

    // 两表Join查询，右表连接键为主键，右表为主键查询
    // 开启了join merge join
    // 右表虽然为二级索引的查询，但Join列不是索引列，应该选择NestLoop
    // 会是一个table1 join ( table2 index join table2 key )的多级join
    @Test
    public void test_两表Join_主键_存在二级索引条件() {
        TableNode table = new TableNode("TABLE1");
        JoinNode join = table.join("TABLE2", "ID", "ID");
        join.query("TABLE2.NAME = 1");
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(join, null, extraCmd);
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertTrue(((IMerge) qc).getSubNode().get(0) instanceof IJoin);
        IJoin subJoin = (IJoin) ((IMerge) qc).getSubNode().get(0);
        Assert.assertTrue(subJoin.getRightNode() instanceof IJoin);
        Assert.assertEquals(JoinStrategy.NEST_LOOP_JOIN, subJoin.getJoinStrategy());
    }

    // 两表Join查询，右表连接键为主键，右表为主键查询
    // 开启了join merge join
    // 右表主键索引的查询，Join列也索引列，应该选择IndexNestLoop
    // 会是一个(table1 join table2 index ) join table2 key 的多级join
    @Test
    public void test_两表Join_主键索引_存在主键索引条件() {
        TableNode table = new TableNode("TABLE1");
        JoinNode join = table.join("TABLE2", "ID", "ID");
        join.query("TABLE2.ID IN (1,2)");
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(join, null, extraCmd);
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertTrue(((IMerge) qc).getSubNode().get(0) instanceof IJoin);
        IJoin subJoin = (IJoin) ((IMerge) qc).getSubNode().get(0);
        Assert.assertEquals(JoinStrategy.INDEX_NEST_LOOP, subJoin.getJoinStrategy());
    }

    // 两表Join查询，右表连接键为主键，右表为二级索引查询
    // 开启了join merge join
    // 右表二级索引的查询，Join列也是二级索引索引，应该选择NestLoop
    // 会是一个(table1 index join table1 index ) join (table2 index join table2
    // key)的多级join
    @Test
    public void test_两表Join_二级索引_存在二级索引条件() {
        TableNode table = new TableNode("TABLE1");
        JoinNode join = table.join("TABLE2", "NAME", "NAME");
        join.query("TABLE2.NAME = 1");
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(join, null, extraCmd);
        Assert.assertTrue(qc instanceof IJoin);
        Assert.assertTrue(((IJoin) qc).getLeftNode() instanceof IJoin);
        Assert.assertTrue(((IJoin) qc).getRightNode() instanceof IMerge);
        Assert.assertEquals(JoinStrategy.INDEX_NEST_LOOP, ((IJoin) qc).getJoinStrategy());
        IJoin subJoin = (IJoin) ((IJoin) qc).getLeftNode();
        Assert.assertTrue(((IJoin) subJoin).getLeftNode() instanceof IMerge);
        Assert.assertTrue(((IJoin) subJoin).getRightNode() instanceof IMerge);
        Assert.assertEquals(JoinStrategy.INDEX_NEST_LOOP, subJoin.getJoinStrategy());
    }

    @Test
    public void test_三表Join_主键索引_存在主键索引条件() {
        TableNode table = new TableNode("TABLE1");
        JoinNode join = table.join("TABLE2", "TABLE1.ID", "TABLE2.ID");
        join = join.join("TABLE3", "TABLE1.ID", "TABLE3.ID");
        join.query("TABLE3.ID IN (1,2)");
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(join, null, extraCmd);
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertTrue(((IMerge) qc).getSubNode().get(0) instanceof IJoin);
        IJoin subJoin = (IJoin) ((IMerge) qc).getSubNode().get(0);
        Assert.assertTrue(subJoin.getLeftNode() instanceof IJoin);
        Assert.assertEquals(JoinStrategy.INDEX_NEST_LOOP, subJoin.getJoinStrategy());
    }

    @Test
    public void test_两表join_orderby_groupby_limit条件() {
        TableNode table = new TableNode("TABLE1");
        JoinNode join = table.join("TABLE2", "ID", "ID");
        join.select(OptimizerUtils.createColumnFromString("TABLE1.ID AS JID"),
            OptimizerUtils.createColumnFromString("CONCAT(TABLE1.NAME,TABLE1.SCHOOL) AS JNAME"));
        join.orderBy("JID");
        join.groupBy("JNAME");
        join.having("COUNT(JID) > 0");
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(join, null, extraCmd);
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertEquals(QUERY_CONCURRENCY.CONCURRENT, ((IMerge) qc).getQueryConcurrency());// 串行
    }

    @Test
    public void test_两表join_单独limit条件_不做并行() {
        TableNode table = new TableNode("TABLE1");
        JoinNode join = table.join("TABLE2", "ID", "ID");
        join.select(OptimizerUtils.createColumnFromString("TABLE1.ID AS JID"),
            OptimizerUtils.createColumnFromString("CONCAT(TABLE1.NAME,TABLE1.SCHOOL) AS JNAME"));
        join.limit(10, 20);
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(join, null, extraCmd);
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertEquals(QUERY_CONCURRENCY.SEQUENTIAL, ((IMerge) qc).getQueryConcurrency());// 串行
        IJoin jn = (IJoin) ((IMerge) qc).getSubNode().get(0);
        Assert.assertEquals("0", jn.getLimitFrom().toString());
        Assert.assertEquals("30", jn.getLimitTo().toString());
    }
}
