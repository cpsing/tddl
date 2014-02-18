package com.taobao.tddl.optimizer.costbased;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.tddl.common.model.ExtraCmd;
import com.taobao.tddl.optimizer.BaseOptimizerTest;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode.FilterType;
import com.taobao.tddl.optimizer.core.ast.query.JoinNode;
import com.taobao.tddl.optimizer.core.ast.query.MergeNode;
import com.taobao.tddl.optimizer.core.ast.query.QueryNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.plan.query.IJoin.JoinStrategy;
import com.taobao.tddl.optimizer.costbased.chooser.IndexChooser;
import com.taobao.tddl.optimizer.costbased.chooser.JoinChooser;
import com.taobao.tddl.optimizer.costbased.pusher.FilterPusher;
import com.taobao.tddl.optimizer.utils.FilterUtils;

public class JoinChooserTest extends BaseOptimizerTest {

    @Test
    public void test_TableNode转化为index_主键索引() {
        TableNode table = new TableNode("TABLE1");
        table.query("ID = 1");
        build(table);

        QueryTreeNode qn = table.convertToJoinIfNeed();

        Assert.assertTrue(qn instanceof TableNode);
        Assert.assertEquals("TABLE1.ID = 1", ((TableNode) qn).getKeyFilter().toString());
    }

    @Test
    public void test_TableNode转化为index_查询字段都在索引上() {
        TableNode table = new TableNode("TABLE1");
        table.select("ID,NAME");
        table.query("NAME = 1");
        build(table);

        QueryTreeNode qn = table.convertToJoinIfNeed();

        Assert.assertTrue(qn instanceof TableNode);
        Assert.assertEquals("TABLE1._NAME.NAME = 1", ((TableNode) qn).getKeyFilter().toString());
    }

    @Test
    public void test_TableNode转化为index到kv的join() {
        // table1存在id的主键索引和name的二级索引，期望构造为index name join id
        TableNode table = new TableNode("TABLE1");
        table.query("NAME = 1");
        build(table);

        QueryTreeNode qn = table.convertToJoinIfNeed();

        Assert.assertTrue(qn instanceof JoinNode);
        Assert.assertEquals("TABLE1._NAME.NAME = 1", ((JoinNode) qn).getLeftNode().getKeyFilter().toString());
        Assert.assertEquals("TABLE1._NAME.ID = TABLE1.ID", ((JoinNode) qn).getJoinFilter().get(0).toString());
    }

    @Test
    public void test_TableNode转化为index到kv_复杂条件查询() {
        TableNode table = new TableNode("TABLE1");
        table.select("(ID + NAME) AS NEWNAME"); // 设置为函数
        table.query("NAME = 1 AND ID > 3 AND SCHOOL = 1");
        table.orderBy("SCHOOL", false);// 增加一个隐藏列
        table.groupBy("NEWNAME");
        build(table);

        QueryTreeNode qn = table.convertToJoinIfNeed();

        Assert.assertTrue(qn instanceof JoinNode);
        Assert.assertEquals("TABLE1._NAME.NAME = 1", ((JoinNode) qn).getLeftNode().getKeyFilter().toString());
        Assert.assertEquals("TABLE1._NAME.ID > 3", ((JoinNode) qn).getLeftNode().getResultFilter().toString());
        Assert.assertEquals("TABLE1.SCHOOL = 1", ((JoinNode) qn).getRightNode().getResultFilter().toString());
        Assert.assertEquals("TABLE1._NAME.ID = TABLE1.ID", ((JoinNode) qn).getJoinFilter().get(0).toString());
    }

    @Test
    public void test_子查询套TableNode转化为index到kv() {
        TableNode table = new TableNode("TABLE1");
        table.query("NAME = 1");
        build(table);

        QueryNode query = new QueryNode(table);
        query.build();

        QueryTreeNode qn = query.convertToJoinIfNeed();
        Assert.assertTrue(qn instanceof QueryNode);
        Assert.assertTrue(qn.getChild() instanceof JoinNode);
        Assert.assertEquals("TABLE1._NAME.NAME = 1", ((JoinNode) qn.getChild()).getLeftNode().getKeyFilter().toString());
        Assert.assertEquals("TABLE1._NAME.ID = TABLE1.ID", ((JoinNode) qn.getChild()).getJoinFilter().get(0).toString());
    }

    @Test
    public void test_Join左是子查询_右是TableNode_转化为最左树() {
        TableNode table1 = new TableNode("TABLE1");
        QueryNode query = new QueryNode(table1);
        query.build();

        TableNode table2 = new TableNode("TABLE2");

        JoinNode join = table1.join(table2, "NAME", "NAME");
        join.setJoinStrategy(JoinStrategy.INDEX_NEST_LOOP);
        join.query("TABLE1.NAME = 1 AND TABLE1.ID > 3 AND TABLE1.SCHOOL = 1");// 原本条件应该是加在join下的，这里省区推导的过程
        join.select("(TABLE2.ID + TABLE2.NAME) AS NEWNAME"); // 设置为函数
        join.orderBy("TABLE1.SCHOOL", false);// 增加一个隐藏列
        join.groupBy("NEWNAME");
        join.build();

        QueryTreeNode qn = FilterPusher.optimize(join);// 先把条件推导子节点上，构建子节点join
        build(table1);
        build(table2);
        qn = qn.convertToJoinIfNeed();

        Assert.assertTrue(qn instanceof JoinNode);
        Assert.assertEquals("TABLE2$_NAME.ID = TABLE2.ID", ((JoinNode) qn).getJoinFilter().get(0).toString());

        Assert.assertTrue(((JoinNode) qn).getLeftNode() instanceof JoinNode);
        Assert.assertEquals("TABLE1.NAME = TABLE2$_NAME.NAME",
            ((JoinNode) ((JoinNode) qn).getLeftNode()).getJoinFilter().get(0).toString());

        Assert.assertTrue(((JoinNode) ((JoinNode) qn).getLeftNode()).getLeftNode() instanceof JoinNode);
        JoinNode jn = (JoinNode) ((JoinNode) ((JoinNode) qn).getLeftNode()).getLeftNode();
        Assert.assertEquals("TABLE1._NAME.NAME = 1", jn.getLeftNode().getKeyFilter().toString());
        Assert.assertEquals("TABLE1._NAME.ID > 3", jn.getLeftNode().getResultFilter().toString());
        Assert.assertEquals("TABLE1.SCHOOL = 1", jn.getRightNode().getResultFilter().toString());
        Assert.assertEquals("TABLE1._NAME.ID = TABLE1.ID", jn.getJoinFilter().get(0).toString());
    }

    /**
     * c1有索引，c2无索引
     */
    @Test
    public void test_JoinStrategy选择_调整join顺序() {
        // table11.c1为二级索引,table10.c2不存在索引
        TableNode table1 = new TableNode("TABLE11");
        TableNode table2 = new TableNode("TABLE10");
        QueryTreeNode qn = table1.join(table2).addJoinKeys("C1", "C2");
        qn.build();
        qn = FilterPusher.optimize(qn);// 先把条件推导子节点上，构建子节点join
        build(table1);
        build(table2);

        qn = optimize(qn, true, true, true);
        Assert.assertEquals(JoinStrategy.INDEX_NEST_LOOP, ((JoinNode) qn).getJoinStrategy());
        Assert.assertTrue(((JoinNode) qn).getLeftNode() instanceof JoinNode);
        Assert.assertEquals("TABLE10", ((JoinNode) ((JoinNode) qn).getLeftNode()).getLeftNode().getName());
        Assert.assertEquals("TABLE11", ((JoinNode) qn).getRightNode().getName());
        Assert.assertEquals("TABLE11", ((TableNode) ((JoinNode) qn).getRightNode()).getIndexUsed().getName());
    }

    @Test
    public void test_JoinStrategy选择_存在条件() {
        // table11虽然是C2非主键列为join，但存在主键条件，使用NESTLOOP
        TableNode table1 = new TableNode("TABLE10");
        TableNode table2 = new TableNode("TABLE11");
        QueryTreeNode qn = table1.join(table2).addJoinKeys("C1", "C2");
        qn.query("TABLE11.ID = 1");
        qn.build();
        qn = FilterPusher.optimize(qn);// 先把条件推导子节点上，构建子节点join
        build(table1);
        build(table2);

        qn = optimize(qn, true, true, true);
        Assert.assertEquals(JoinStrategy.NEST_LOOP_JOIN, ((JoinNode) qn).getJoinStrategy());
        Assert.assertEquals("TABLE10", ((JoinNode) qn).getLeftNode().getName());
        Assert.assertEquals("TABLE11", ((JoinNode) qn).getRightNode().getName());
        Assert.assertEquals("TABLE11", ((TableNode) ((JoinNode) qn).getRightNode()).getIndexUsed().getName());
    }

    @Test
    public void test_JoinStrategy选择_存在子查询_存在条件() {
        // table11虽然是C2非主键列为join，但存在主键条件，使用NESTLOOP
        TableNode table1 = new TableNode("TABLE10");
        QueryNode query = new QueryNode(table1);

        TableNode table2 = new TableNode("TABLE11");
        QueryTreeNode qn = query.join(table2).addJoinKeys("C1", "C2");
        qn.query("TABLE11.ID = 1");
        qn.build();
        qn = FilterPusher.optimize(qn);// 先把条件推导子节点上，构建子节点join
        build(table1);
        build(table2);

        qn = optimize(qn, true, true, true);
        Assert.assertEquals(JoinStrategy.NEST_LOOP_JOIN, ((JoinNode) qn).getJoinStrategy());
        Assert.assertEquals("TABLE10", ((JoinNode) qn).getLeftNode().getName());
        Assert.assertEquals("TABLE11", ((JoinNode) qn).getRightNode().getName());
        Assert.assertEquals("TABLE11", ((TableNode) ((JoinNode) qn).getRightNode()).getIndexUsed().getName());
    }

    @Test
    public void test_存在OR条件_所有字段均有索引_构建IndexMerge() {
        TableNode table1 = new TableNode("TABLE10");
        table1.query("ID = 1 OR C1 = 2");
        table1.build();
        QueryTreeNode qn = optimize(table1, true, true, true);
        Assert.assertTrue(qn instanceof MergeNode);
        Assert.assertTrue(qn.getChildren().get(0) instanceof TableNode);
        Assert.assertTrue(qn.getChildren().get(1) instanceof JoinNode);
    }

    @Test
    public void test_存在OR条件_有字段无索引_构建全表扫描() {
        TableNode table1 = new TableNode("TABLE10");
        table1.query("ID = 1 OR C1 = 2 OR C2 = 1");
        table1.build();
        QueryTreeNode qn = optimize(table1, true, true, false);
        Assert.assertTrue(qn instanceof TableNode);
        // Assert.assertTrue(((TableNode) qn).isFullTableScan());
    }

    @Test
    public void test_JoinStrategy选择sortmerge_outterJoin() {
        TableNode table1 = new TableNode("TABLE10");
        TableNode table2 = new TableNode("TABLE11");
        JoinNode join = table1.join(table2).setOuterJoin();
        join.build();

        QueryTreeNode qn = optimize(join, true, true, true);
        Assert.assertEquals(JoinStrategy.SORT_MERGE_JOIN, ((JoinNode) qn).getJoinStrategy());
    }

    @Test
    public void test_JoinStrategy选择sortmerge_右表存在group条件() {
        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");
        JoinNode join = table1.join(table2).setLeftOuterJoin().addJoinKeys("ID", "ID").addJoinKeys("NAME", "NAME");
        join.orderBy("TABLE2.ID");
        join.groupBy("TABLE2.NAME").groupBy("TABLE2.ID").groupBy("TABLE2.SCHOOL");
        join.build();

        QueryTreeNode qn = optimize(join, true, true, true);
        Assert.assertEquals(JoinStrategy.SORT_MERGE_JOIN, ((JoinNode) qn).getJoinStrategy());
    }

    @Test
    public void test_JoinStrategy选择sortmerge_右表存在group条件和order条件不能合并() {
        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");
        JoinNode join = table1.join(table2).setLeftOuterJoin().addJoinKeys("ID", "ID").addJoinKeys("NAME", "NAME");
        join.orderBy("TABLE2.SCHOOL");
        join.groupBy("TABLE2.NAME").groupBy("TABLE2.ID");
        join.build();

        QueryTreeNode qn = optimize(join, true, true, true);
        Assert.assertEquals(JoinStrategy.SORT_MERGE_JOIN, ((JoinNode) qn).getJoinStrategy());
    }

    @Test
    public void test_JoinStrategy选择sortmerge_右表存在order条件() {
        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");
        JoinNode join = table1.join(table2).setLeftOuterJoin().addJoinKeys("ID", "ID").addJoinKeys("NAME", "NAME");
        join.orderBy("TABLE2.ID");
        join.build();

        QueryTreeNode qn = optimize(join, true, true, true);
        Assert.assertEquals(JoinStrategy.SORT_MERGE_JOIN, ((JoinNode) qn).getJoinStrategy());
    }

    @Test
    public void test_JoinStrategy不选择sortmerge_右表存在order条件() {
        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");
        JoinNode join = table1.join(table2).setLeftOuterJoin().addJoinKeys("ID", "ID").addJoinKeys("NAME", "NAME");
        join.orderBy("TABLE2.SCHOOL"); // join列的顺序没法推导
        join.build();

        QueryTreeNode qn = optimize(join, true, true, true);
        Assert.assertEquals(JoinStrategy.INDEX_NEST_LOOP, ((JoinNode) qn).getJoinStrategy());
    }

    private QueryTreeNode optimize(QueryTreeNode qtn, boolean chooseIndex, boolean chooseJoin, boolean chooseIndexMerge) {
        Map<String, Object> extraCmd = new HashMap<String, Object>();
        extraCmd.put(ExtraCmd.CHOOSE_INDEX, chooseIndex);
        extraCmd.put(ExtraCmd.CHOOSE_JOIN, chooseJoin);
        extraCmd.put(ExtraCmd.CHOOSE_INDEX_MERGE, chooseIndexMerge);
        return (QueryTreeNode) JoinChooser.optimize(qtn, extraCmd);
    }

    private void build(TableNode table) {
        table.build();

        Map<String, Object> extraCmd = new HashMap<String, Object>();
        extraCmd.put(ExtraCmd.CHOOSE_INDEX, true);
        IndexMeta index = IndexChooser.findBestIndex(table.getTableMeta(),
            new ArrayList<ISelectable>(),
            FilterUtils.toDNFNode(table.getWhereFilter()),
            table.getTableName(),
            extraCmd);

        table.useIndex(index);

        Map<FilterType, IFilter> result = FilterSpliter.splitByIndex(FilterUtils.toDNFNode(table.getWhereFilter()),
            table);

        table.setKeyFilter(result.get(FilterType.IndexQueryKeyFilter));
        table.setIndexQueryValueFilter(result.get(FilterType.IndexQueryValueFilter));
        table.setResultFilter(result.get(FilterType.ResultFilter));
    }

}
