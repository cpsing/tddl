package com.taobao.tddl.optimizer.costbased;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.tddl.common.model.ExtraCmd;
import com.taobao.tddl.optimizer.BaseOptimizerTest;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.query.JoinNode;
import com.taobao.tddl.optimizer.core.ast.query.KVIndexNode;
import com.taobao.tddl.optimizer.core.ast.query.MergeNode;
import com.taobao.tddl.optimizer.core.ast.query.QueryNode;
import com.taobao.tddl.optimizer.costbased.chooser.DataNodeChooser;

public class DataNodeChooserTest extends BaseOptimizerTest {

    @Test
    public void test_单表查询_不生成merge() {
        KVIndexNode table7 = new KVIndexNode("TABLE7");
        table7.build();
        QueryTreeNode qtn = shard(table7, false, false);

        Assert.assertTrue(qtn instanceof KVIndexNode);
        Assert.assertEquals("andor_group_0", qtn.getDataNode());
        Assert.assertEquals("table7", ((KVIndexNode) qtn).getActualTableName());
    }

    @Test
    public void test_单表查询_生成merge() {
        KVIndexNode table7 = new KVIndexNode("TABLE1");
        table7.build();
        QueryTreeNode qtn = shard(table7, false, false);

        Assert.assertTrue(qtn instanceof MergeNode);
        Assert.assertEquals(8, qtn.getChildren().size());
    }

    @Test
    public void test_子查询_不生成merge() {
        KVIndexNode table7 = new KVIndexNode("TABLE7");
        table7.build();

        QueryNode query = new QueryNode(table7);
        QueryTreeNode qtn = shard(query, false, false);

        Assert.assertTrue(qtn instanceof QueryNode);
        Assert.assertEquals("andor_group_0", qtn.getDataNode());
        Assert.assertEquals("andor_group_0", qtn.getChild().getDataNode());
        Assert.assertEquals("table7", ((KVIndexNode) qtn.getChild()).getActualTableName());
    }

    @Test
    public void test_子查询_生成merge() {
        KVIndexNode table7 = new KVIndexNode("TABLE1");
        table7.build();

        QueryNode query = new QueryNode(table7);
        QueryTreeNode qtn = shard(query, false, false);

        // Merge的query
        Assert.assertTrue(qtn instanceof MergeNode);
        Assert.assertTrue(qtn.getChild() instanceof QueryNode);
        Assert.assertEquals(8, qtn.getChildren().size());
    }

    @Test
    public void test_子查询_不生成merge_存在聚合查询() {
        KVIndexNode table7 = new KVIndexNode("TABLE1");
        table7.limit(0, 1);
        table7.build();

        QueryNode query = new QueryNode(table7);
        QueryTreeNode qtn = shard(query, false, false);

        Assert.assertTrue(qtn instanceof QueryNode);
        Assert.assertTrue(qtn.getChild() instanceof MergeNode);
        Assert.assertEquals(8, ((MergeNode) qtn.getChild()).getChildren().size());
    }

    @Test
    public void test_join查询_不生成merge() {
        KVIndexNode table1 = new KVIndexNode("TABLE1");
        table1.keyQuery("ID = 1");
        KVIndexNode table2 = new KVIndexNode("TABLE2");
        table2.keyQuery("ID = 1");

        JoinNode join = table1.join(table2, "ID", "ID");
        join.build();
        QueryTreeNode qtn = shard(join, false, false);

        Assert.assertTrue(qtn instanceof JoinNode);
        Assert.assertEquals("andor_group_0", ((JoinNode) qtn).getLeftNode().getDataNode());
        Assert.assertEquals("andor_group_0", ((JoinNode) qtn).getRightNode().getDataNode());
        Assert.assertEquals("table1_01", ((KVIndexNode) ((JoinNode) qtn).getLeftNode()).getActualTableName());
        Assert.assertEquals("table2_01", ((KVIndexNode) ((JoinNode) qtn).getRightNode()).getActualTableName());
    }

    @Test
    public void test_join查询_生成MergeJoinMerge_没设置条件全表扫描() {
        KVIndexNode table1 = new KVIndexNode("TABLE1");
        KVIndexNode table2 = new KVIndexNode("TABLE2");

        JoinNode join = table1.join(table2, "ID", "ID");
        join.build();
        QueryTreeNode qtn = shard(join, false, false);

        Assert.assertTrue(qtn instanceof JoinNode);
        Assert.assertTrue(((JoinNode) qtn).getLeftNode() instanceof MergeNode);
        Assert.assertTrue(((JoinNode) qtn).getRightNode() instanceof MergeNode);
        Assert.assertEquals(8, ((JoinNode) qtn).getLeftNode().getChildren().size());
        Assert.assertEquals(1, ((JoinNode) qtn).getRightNode().getChildren().size());// 右边是一个未决节点
    }

    @Test
    public void test_join查询_生成JoinMergeJoin_设置filter局部表join_开启优化参数() {
        KVIndexNode table1 = new KVIndexNode("TABLE1");
        table1.keyQuery("TABLE1.ID IN (1,2,3)");
        KVIndexNode table2 = new KVIndexNode("TABLE2");
        table2.keyQuery("TABLE2.ID IN (1,2,3)");

        JoinNode join = table1.join(table2, "ID", "ID");
        join.build();
        QueryTreeNode qtn = shard(join, false, true);

        Assert.assertTrue(qtn instanceof MergeNode);
        Assert.assertEquals(3, qtn.getChildren().size());
        Assert.assertTrue(qtn.getChild() instanceof JoinNode);// join merge join
    }

    @Test
    public void test_join查询_生成JoinMergeJoin_没设置条件全表扫描_开启优化参数() {
        KVIndexNode table1 = new KVIndexNode("TABLE1");
        KVIndexNode table2 = new KVIndexNode("TABLE2");

        JoinNode join = table1.join(table2, "ID", "ID");
        join.build();
        QueryTreeNode qtn = shard(join, false, true);

        Assert.assertTrue(qtn instanceof MergeNode);
        Assert.assertEquals(8, qtn.getChildren().size());
        Assert.assertTrue(qtn.getChild() instanceof JoinNode);// join merge join
    }

    @Test
    public void test_join查询_不生成JoinMergeJoin_join条件不是分区字段_开启优化参数() {
        KVIndexNode table1 = new KVIndexNode("TABLE1");
        KVIndexNode table2 = new KVIndexNode("TABLE2");

        JoinNode join = table1.join(table2, "NAME", "NAME");
        join.build();
        QueryTreeNode qtn = shard(join, false, true);

        Assert.assertTrue(qtn instanceof JoinNode); // 不做join merge join优化
        Assert.assertTrue(((JoinNode) qtn).getLeftNode() instanceof MergeNode);
        Assert.assertTrue(((JoinNode) qtn).getRightNode() instanceof MergeNode);
        Assert.assertEquals(8, ((JoinNode) qtn).getLeftNode().getChildren().size());
        Assert.assertEquals(1, ((JoinNode) qtn).getRightNode().getChildren().size());// 右边是一个未决节点
    }

    @Test
    public void test_join查询_生成JoinMergeJoin_嵌套子查询和Join带条件局部join_开启优化参数() {
        KVIndexNode table1 = new KVIndexNode("TABLE1");
        table1.alias("A");
        QueryNode query1 = new QueryNode(table1);
        query1.select("A.ID AS AID , A.NAME AS ANAME , A.SCHOOL AS ASCHOOL");

        KVIndexNode table2 = new KVIndexNode("TABLE2");
        table2.alias("B");
        QueryNode query2 = new QueryNode(table2);
        query2.select("B.ID AS BID , B.NAME AS BNAME , B.SCHOOL AS BSCHOOL");

        JoinNode join = query1.join(query2, "AID", "BID");// 用的是子表的别名
        join.build();
        QueryTreeNode qtn = shard(join, false, true);

        Assert.assertTrue(qtn instanceof MergeNode);
        Assert.assertEquals(8, qtn.getChildren().size());
        Assert.assertTrue(qtn.getChild() instanceof JoinNode);// join merge join
        Assert.assertTrue(((JoinNode) qtn.getChild()).getChild() instanceof QueryNode);
    }

    @Test
    public void test_join查询_生成JoinMergeJoin_嵌套子查询全表扫描_开启优化参数() {
        KVIndexNode table1 = new KVIndexNode("TABLE1");
        table1.keyQuery("TABLE1.ID IN (1,2,3)");
        table1.alias("A");
        QueryNode query1 = new QueryNode(table1);
        query1.select("A.ID AS AID , A.NAME AS ANAME , A.SCHOOL AS ASCHOOL");

        KVIndexNode table2 = new KVIndexNode("TABLE2");
        table2.keyQuery("TABLE2.ID IN (1,2,3)");

        KVIndexNode table3 = new KVIndexNode("TABLE3");
        table3.keyQuery("TABLE3.ID IN (1,2,3)");

        JoinNode join = table2.join(table3, "ID", "ID");

        // 两层join，左边是query，右边是join
        JoinNode nextJoin = query1.join(join, "AID", "TABLE2.ID");// 用的是子表的别名
        nextJoin.build();
        QueryTreeNode qtn = shard(nextJoin, false, true);

        Assert.assertTrue(qtn instanceof MergeNode);
        Assert.assertEquals(3, qtn.getChildren().size());
        Assert.assertTrue(qtn.getChild() instanceof JoinNode);// join merge join
        Assert.assertTrue(((JoinNode) qtn.getChild()).getLeftNode() instanceof QueryNode);
        Assert.assertTrue(((JoinNode) qtn.getChild()).getRightNode() instanceof JoinNode);
    }

    @Test
    public void test_join查询_不生成JoinMergeJoin_嵌套子查询全表扫描_存在聚合查询_开启优化参数() {
        KVIndexNode table1 = new KVIndexNode("TABLE1");
        table1.keyQuery("TABLE1.ID IN (1,2,3)");
        table1.alias("A");
        table1.limit(0, 1);// 需要聚合操作
        QueryNode query1 = new QueryNode(table1);
        query1.select("A.ID AS AID , A.NAME AS ANAME , A.SCHOOL AS ASCHOOL");

        KVIndexNode table2 = new KVIndexNode("TABLE2");
        table2.keyQuery("TABLE2.ID IN (1,2,3)");

        KVIndexNode table3 = new KVIndexNode("TABLE3");
        table3.keyQuery("TABLE3.ID IN (1,2,3)");

        JoinNode join = table2.join(table3, "ID", "ID");

        // 两层join，左边是query，右边是join
        JoinNode nextJoin = query1.join(join, "AID", "TABLE2.ID");// 用的是子表的别名
        nextJoin.build();
        QueryTreeNode qtn = shard(nextJoin, false, true);

        Assert.assertTrue(qtn instanceof JoinNode);
        Assert.assertTrue(((JoinNode) qtn).getLeftNode() instanceof QueryNode);
        Assert.assertEquals(3, ((MergeNode) ((JoinNode) qtn).getLeftNode().getChild()).getChildren().size());
        Assert.assertTrue(((JoinNode) qtn).getRightNode() instanceof MergeNode);
    }

    @Test
    public void test_join查询_不生成JoinMergeJoin_左边是不是Merge_开启优化参数() {
        KVIndexNode table1 = new KVIndexNode("TABLE8");// 这是一个单表，不分库
        table1.keyQuery("TABLE8.ID IN (1,2,3)");
        KVIndexNode table2 = new KVIndexNode("TABLE2");// 分库表
        table2.keyQuery("TABLE2.ID IN (1,2,3)");

        JoinNode join = table1.join(table2, "ID", "ID");
        join.build();
        QueryTreeNode qtn = shard(join, false, true);

        Assert.assertTrue(qtn instanceof JoinNode);
        Assert.assertTrue(((JoinNode) qtn).getLeftNode() instanceof KVIndexNode);
        Assert.assertTrue(((JoinNode) qtn).getRightNode() instanceof MergeNode);
        Assert.assertEquals(1, ((JoinNode) qtn).getRightNode().getChildren().size());// 因为是个未决节点
    }

    @Test
    public void test_join查询_不生成JoinMergeJoin_左边是不是Merge_右边是子查询_开启优化参数() {
        KVIndexNode table1 = new KVIndexNode("TABLE8");// 这是一个单表，不分库
        table1.keyQuery("TABLE8.ID IN (1,2,3)");
        KVIndexNode table2 = new KVIndexNode("TABLE2");// 分库表
        table2.keyQuery("TABLE2.ID IN (1,2,3)");
        QueryNode query = new QueryNode(table2);
        query.setSubQuery(true); // 是个子查询，会进行BLOCK_NEST_LOOP处理，就不需要未决节点进行mget处理

        JoinNode join = table1.join(query, "ID", "ID");
        join.build();
        QueryTreeNode qtn = shard(join, false, true);

        Assert.assertTrue(qtn instanceof JoinNode);
        Assert.assertTrue(((JoinNode) qtn).getLeftNode() instanceof KVIndexNode);
        Assert.assertTrue(((JoinNode) qtn).getRightNode() instanceof MergeNode);
        Assert.assertTrue(((JoinNode) qtn).getRightNode().getChild() instanceof QueryNode);
        Assert.assertEquals(3, ((JoinNode) qtn).getRightNode().getChildren().size());// 是个子查询的Merge
    }

    @Test
    public void test_join查询_不生成JoinMergeJoin_左右分区结果不一致_开启优化参数() {
        KVIndexNode table1 = new KVIndexNode("TABLE1");// 这是一个单表，不分库
        table1.keyQuery("TABLE1.ID IN (2,3,4)");
        KVIndexNode table2 = new KVIndexNode("TABLE2");// 分库表
        table2.keyQuery("TABLE2.ID IN (1,2,3)");

        JoinNode join = table1.join(table2, "ID", "ID");
        join.build();
        QueryTreeNode qtn = shard(join, false, true);

        Assert.assertTrue(qtn instanceof JoinNode);
        Assert.assertTrue(((JoinNode) qtn).getLeftNode() instanceof MergeNode);
        Assert.assertTrue(((JoinNode) qtn).getRightNode() instanceof MergeNode);
        Assert.assertEquals(3, ((JoinNode) qtn).getLeftNode().getChildren().size());
        Assert.assertEquals(1, ((JoinNode) qtn).getRightNode().getChildren().size());// 未决节点
    }

    @Test
    public void test_join查询_生成JoinMergeJoin_左边是广播表_开启优化参数() {
        KVIndexNode table1 = new KVIndexNode("TABLE7");// 这是一个广播表
        QueryNode query = new QueryNode(table1);

        KVIndexNode table2 = new KVIndexNode("TABLE2");// 分库表

        JoinNode join = query.join(table2, "ID", "ID");
        join.build();
        QueryTreeNode qtn = shard(join, false, true);

        Assert.assertTrue(qtn instanceof MergeNode);
        Assert.assertEquals(8, qtn.getChildren().size());
        Assert.assertTrue(qtn.getChild() instanceof JoinNode);// join merge join
        Assert.assertTrue(((JoinNode) qtn.getChild()).getLeftNode() instanceof QueryNode);
        Assert.assertTrue(((JoinNode) qtn.getChild()).getRightNode() instanceof KVIndexNode);
    }

    @Test
    public void test_join查询_生成JoinMergeJoin_右边是广播表_开启优化参数() {
        KVIndexNode table1 = new KVIndexNode("TABLE1");// 分库表
        QueryNode query = new QueryNode(table1);

        KVIndexNode table2 = new KVIndexNode("TABLE7");// 这是一个广播表

        JoinNode join = query.join(table2, "ID", "ID");
        join.build();
        QueryTreeNode qtn = shard(join, false, true);

        Assert.assertTrue(qtn instanceof MergeNode);
        Assert.assertEquals(8, qtn.getChildren().size());
        Assert.assertTrue(qtn.getChild() instanceof JoinNode);// join merge join
        Assert.assertTrue(((JoinNode) qtn.getChild()).getLeftNode() instanceof QueryNode);
        Assert.assertTrue(((JoinNode) qtn.getChild()).getRightNode() instanceof KVIndexNode);
    }

    @Test
    public void test_join查询_不生成JoinMergeJoin_左右都是广播表_开启优化参数() {
        KVIndexNode table1 = new KVIndexNode("TABLE7");// 这是一个广播表
        table1.alias("A");
        QueryNode query = new QueryNode(table1);

        KVIndexNode table2 = new KVIndexNode("TABLE7");// 这是一个广播表
        table2.alias("B");

        JoinNode join = query.join(table2, "ID", "ID");
        join.build();
        QueryTreeNode qtn = shard(join, false, true);

        Assert.assertTrue(qtn instanceof JoinNode);
        Assert.assertTrue(((JoinNode) qtn).getLeftNode() instanceof QueryNode);
        Assert.assertTrue(((JoinNode) qtn).getRightNode() instanceof KVIndexNode);
    }

    @Test
    public void test_join查询_不生成JoinMergeJoin_不同的joinGroup_开启优化参数() {
        KVIndexNode table1 = new KVIndexNode("TABLE1");// 分库表
        QueryNode query = new QueryNode(table1);

        KVIndexNode table2 = new KVIndexNode("TABLE5");// 这是一个广播表

        JoinNode join = query.join(table2, "ID", "ID");
        join.build();
        QueryTreeNode qtn = shard(join, false, true);

        Assert.assertTrue(qtn instanceof JoinNode);
        Assert.assertTrue(((JoinNode) qtn).getLeftNode() instanceof MergeNode);
        Assert.assertTrue(((JoinNode) qtn).getRightNode() instanceof MergeNode);
        Assert.assertEquals(8, ((JoinNode) qtn).getLeftNode().getChildren().size());
        Assert.assertEquals(1, ((JoinNode) qtn).getRightNode().getChildren().size());// 未决节点
    }

    @Test
    public void test_join查询_生成JoinMergeJoin_左右joinGroup都是other_开启优化参数() {
        KVIndexNode table1 = new KVIndexNode("TABLE5");// 分库表
        QueryNode query = new QueryNode(table1);

        KVIndexNode table2 = new KVIndexNode("TABLE6");// 这是一个广播表

        JoinNode join = query.join(table2, "ID", "ID");
        join.build();
        QueryTreeNode qtn = shard(join, false, true);

        Assert.assertTrue(qtn instanceof MergeNode);
        Assert.assertEquals(4, qtn.getChildren().size());
        Assert.assertTrue(qtn.getChild() instanceof JoinNode);// join merge join
        Assert.assertTrue(((JoinNode) qtn.getChild()).getLeftNode() instanceof QueryNode);
        Assert.assertTrue(((JoinNode) qtn.getChild()).getRightNode() instanceof KVIndexNode);
    }

    private QueryTreeNode shard(QueryTreeNode qtn, boolean joinMergeJoin, boolean joinMergeJoinByRule) {
        Map<String, Object> extraCmd = new HashMap<String, Object>();
        extraCmd.put(ExtraCmd.JOIN_MERGE_JOIN, joinMergeJoin);
        extraCmd.put(ExtraCmd.JOIN_MERGE_JOIN_JUDGE_BY_RULE, joinMergeJoinByRule);
        return (QueryTreeNode) DataNodeChooser.shard(qtn, null, extraCmd);
    }
}
