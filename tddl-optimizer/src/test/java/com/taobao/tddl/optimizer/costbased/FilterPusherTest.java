package com.taobao.tddl.optimizer.costbased;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.tddl.optimizer.BaseOptimizerTest;
import com.taobao.tddl.optimizer.core.ast.query.JoinNode;
import com.taobao.tddl.optimizer.core.ast.query.QueryNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.costbased.pusher.FilterPusher;
import com.taobao.tddl.optimizer.utils.FilterUtils;
import com.taobao.tddl.optimizer.utils.OptimizerUtils;

public class FilterPusherTest extends BaseOptimizerTest {

    @Test
    public void test_where中OR条件不可下推() {
        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");

        JoinNode join = table1.join(table2);
        join.query("(TABLE1.ID>5 OR TABLE2.ID<10) AND TABLE1.NAME = TABLE2.NAME");
        join.build();
        FilterPreProcessor.optimize(join, true);
        FilterPusher.optimize(join);

        Assert.assertEquals(null, join.getLeftNode().getWhereFilter());
        Assert.assertEquals(null, join.getRightNode().getWhereFilter());
        Assert.assertTrue(join.getJoinFilter().isEmpty());
    }

    @Test
    public void test_where条件下推() {
        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");

        JoinNode join = table1.join(table2);
        join.query("TABLE1.ID>5 AND TABLE2.ID<10 AND TABLE1.NAME = TABLE2.NAME");
        join.build();
        FilterPusher.optimize(join);

        Assert.assertEquals("TABLE1.ID > 5", join.getLeftNode().getWhereFilter().toString());
        Assert.assertEquals("TABLE2.ID < 10", join.getRightNode().getWhereFilter().toString());
        Assert.assertEquals("TABLE1.NAME = TABLE2.NAME", join.getJoinFilter().get(0).toString());
    }

    @Test
    public void test_where条件下推_join列存在函数不处理() {
        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");

        JoinNode join = table1.join(table2);
        join.query("TABLE1.ID>5 AND TABLE2.ID<10 AND TABLE1.NAME = TABLE2.NAME + 1");
        join.build();
        FilterPusher.optimize(join);

        Assert.assertEquals("TABLE1.ID > 5", join.getLeftNode().getWhereFilter().toString());
        Assert.assertEquals("TABLE2.ID < 10", join.getRightNode().getWhereFilter().toString());
        Assert.assertEquals("TABLE1.NAME = TABLE2.NAME + 1", join.getWhereFilter().toString());// 还是留在where中
    }

    @Test
    public void test_where条件下推_条件推导下推() {
        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");

        JoinNode join = table1.join(table2);
        join.query("TABLE1.ID>5 AND TABLE2.ID<10 AND TABLE1.ID = TABLE2.ID");
        join.build();
        FilterPusher.optimize(join);

        Assert.assertEquals("(TABLE1.ID > 5 AND TABLE1.ID < 10)", join.getLeftNode().getWhereFilter().toString());
        Assert.assertEquals("(TABLE2.ID < 10 AND TABLE2.ID > 5)", join.getRightNode().getWhereFilter().toString());
        Assert.assertEquals("TABLE1.ID = TABLE2.ID", join.getJoinFilter().get(0).toString());
    }

    @Test
    public void test_where条件下推_子查询() {
        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");

        JoinNode join = table1.join(table2);
        join.alias("S").select("TABLE1.ID,TABLE1.NAME,TABLE1.SCHOOL");
        join.query("TABLE1.ID>5 AND TABLE2.ID<10 AND TABLE1.NAME = TABLE2.NAME");
        join.build();

        QueryNode query = new QueryNode(join);
        query.query("S.SCHOOL = 6");
        query.build();
        FilterPusher.optimize(query);
        Assert.assertEquals("(TABLE1.SCHOOL = 6 AND TABLE1.ID > 5)", join.getLeftNode().getWhereFilter().toString());
        Assert.assertEquals("TABLE2.ID < 10", join.getRightNode().getWhereFilter().toString());
        Assert.assertEquals("TABLE1.NAME = TABLE2.NAME", join.getJoinFilter().get(0).toString());
    }

    @Test
    public void test_where条件下推_子查询_函数列不传递() {
        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");

        JoinNode join = table1.join(table2);
        join.alias("S").select("TABLE1.ID");
        join.getColumnsSelected()
            .add(OptimizerUtils.createColumnFromString("CONCAT_WS(' ',TABLE1.NAME,TABLE1.SCHOOL) AS NAME"));
        join.query("TABLE1.ID>5 AND TABLE2.ID<10 AND TABLE1.NAME = TABLE2.NAME");
        join.build();

        QueryNode query = new QueryNode(join);
        query.query("S.NAME = 1");
        query.build();
        FilterPusher.optimize(query);
        Assert.assertEquals("TABLE1.ID > 5", join.getLeftNode().getWhereFilter().toString());
        Assert.assertEquals("TABLE2.ID < 10", join.getRightNode().getWhereFilter().toString());
        Assert.assertEquals("TABLE1.NAME = TABLE2.NAME", join.getJoinFilter().get(0).toString());
    }

    @Test
    public void test_where条件下推_多级子查询_函数列不传递_字段列传递() {
        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");

        JoinNode join = table1.join(table2);
        join.alias("S").select("TABLE1.ID");
        join.getColumnsSelected()
            .add(OptimizerUtils.createColumnFromString("CONCAT_WS(' ',TABLE1.NAME,TABLE1.SCHOOL) AS NAME"));
        join.query("TABLE1.ID>5 AND TABLE2.ID<10 AND TABLE1.NAME = TABLE2.NAME");
        join.build();

        QueryNode query = new QueryNode(join);
        query.alias("B").query("S.NAME = 1");
        query.build();

        QueryNode nextQuery = new QueryNode(query);
        nextQuery.query("B.ID = 6");
        nextQuery.build();

        FilterPusher.optimize(nextQuery);
        Assert.assertEquals("(TABLE1.ID = 6 AND TABLE1.ID > 5)", join.getLeftNode().getWhereFilter().toString());
        Assert.assertEquals("TABLE2.ID < 10", join.getRightNode().getWhereFilter().toString());
        Assert.assertEquals("TABLE1.NAME = TABLE2.NAME", join.getJoinFilter().get(0).toString());
    }

    @Test
    public void test_where条件下推_多级join() {
        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");
        TableNode table3 = new TableNode("TABLE3");

        JoinNode join = table1.join(table2);
        join.select("TABLE1.ID AS ID , TABLE1.NAME AS NAME");
        join.query("TABLE1.ID>5 AND TABLE2.ID<10 AND TABLE1.NAME = TABLE2.NAME");
        join.build();

        JoinNode nextJoin = join.join(table3);
        nextJoin.query("TABLE1.NAME = 6 AND TABLE1.ID = TABLE3.ID");
        nextJoin.build();
        FilterPusher.optimize(nextJoin);

        Assert.assertEquals("(TABLE1.NAME = 6 AND TABLE1.ID > 5)", ((JoinNode) nextJoin.getLeftNode()).getLeftNode()
            .getWhereFilter()
            .toString());
        Assert.assertEquals("(TABLE2.ID < 10 AND TABLE2.NAME = 6)", ((JoinNode) nextJoin.getLeftNode()).getRightNode()
            .getWhereFilter()
            .toString());
        Assert.assertEquals("TABLE1.ID = TABLE3.ID", nextJoin.getJoinFilter().get(0).toString());
    }

    @Test
    public void test_where条件下推_多级join_子查询() {
        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");

        JoinNode join = table1.join(table2);
        join.alias("S").select("TABLE1.ID AS ID , TABLE1.NAME AS NAME , TABLE2.SCHOOL AS SCHOOL");
        join.query("TABLE1.ID>5 AND TABLE2.ID<10 AND TABLE1.NAME = TABLE2.NAME");
        join.build();

        QueryNode queryA = new QueryNode(join);
        queryA.alias("B").query("S.NAME = 2");
        queryA.build();

        QueryNode queryB = queryA.deepCopy();
        queryB.alias("C").query("S.NAME = 3");
        queryB.build();

        JoinNode nextJoin = queryA.join(queryB);
        nextJoin.query("C.SCHOOL = 4 AND B.ID = C.ID");
        nextJoin.build();
        FilterPusher.optimize(nextJoin);

        Assert.assertEquals("B.ID = C.ID", nextJoin.getJoinFilter().get(0).toString());
        Assert.assertEquals("(TABLE1.NAME = 2 AND TABLE1.ID > 5)",
            ((JoinNode) nextJoin.getLeftNode().getChild()).getLeftNode().getWhereFilter().toString());
        Assert.assertEquals("(TABLE2.ID < 10 AND TABLE2.NAME = 2)",
            ((JoinNode) nextJoin.getLeftNode().getChild()).getRightNode().getWhereFilter().toString());
        Assert.assertEquals("TABLE1.NAME = TABLE2.NAME", ((JoinNode) nextJoin.getLeftNode().getChild()).getJoinFilter()
            .get(0)
            .toString());

        Assert.assertEquals("(TABLE1.NAME = 3 AND TABLE1.ID > 5)",
            ((JoinNode) nextJoin.getRightNode().getChild()).getLeftNode().getWhereFilter().toString());
        Assert.assertEquals("(TABLE2.SCHOOL = 4 AND TABLE2.ID < 10 AND TABLE2.NAME = 3)",
            ((JoinNode) nextJoin.getRightNode().getChild()).getRightNode().getWhereFilter().toString());
        Assert.assertEquals("TABLE1.NAME = TABLE2.NAME",
            ((JoinNode) nextJoin.getRightNode().getChild()).getJoinFilter().get(0).toString());
    }

    @Test
    public void test_join条件下推() {
        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");

        JoinNode join = table1.join(table2).addJoinKeys("TABLE1.NAME", "TABLE2.NAME");
        addOtherJoinFilter(join, "TABLE1.ID>5 AND TABLE2.ID<10");
        join.build();
        FilterPusher.optimize(join);

        Assert.assertEquals("TABLE1.ID > 5", join.getLeftNode().getOtherJoinOnFilter().toString());
        Assert.assertEquals("TABLE2.ID < 10", join.getRightNode().getOtherJoinOnFilter().toString());
        Assert.assertEquals("TABLE1.NAME = TABLE2.NAME", join.getJoinFilter().get(0).toString());
    }

    @Test
    public void test_join条件下推_条件推导下推() {
        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");

        JoinNode join = table1.join(table2).addJoinKeys("TABLE1.ID", "TABLE2.ID");
        addOtherJoinFilter(join, "TABLE1.ID>5 AND TABLE2.ID<10");
        join.build();
        FilterPusher.optimize(join);

        Assert.assertEquals("(TABLE1.ID > 5 AND TABLE1.ID < 10)", join.getLeftNode().getOtherJoinOnFilter().toString());
        Assert.assertEquals("(TABLE2.ID < 10 AND TABLE2.ID > 5)", join.getRightNode().getOtherJoinOnFilter().toString());
        Assert.assertEquals("TABLE1.ID = TABLE2.ID", join.getJoinFilter().get(0).toString());
    }

    @Test
    public void test_join条件下推_多级join() {
        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");
        TableNode table3 = new TableNode("TABLE3");

        JoinNode join = table1.join(table2).addJoinKeys("TABLE1.NAME", "TABLE2.NAME");
        join.alias("A").select("TABLE1.ID AS ID , TABLE1.NAME AS NAME");
        addOtherJoinFilter(join, "TABLE1.ID>5 AND TABLE2.ID<10");
        join.build();

        JoinNode nextJoin = join.join(table3).addJoinKeys("A.ID", "TABLE3.ID");
        addOtherJoinFilter(nextJoin, "A.NAME = 6");
        nextJoin.build();
        FilterPusher.optimize(nextJoin);

        Assert.assertEquals("A.ID = TABLE3.ID", nextJoin.getJoinFilter().get(0).toString());
        Assert.assertEquals("TABLE1.NAME = TABLE2.NAME", ((JoinNode) nextJoin.getLeftNode()).getJoinFilter()
            .get(0)
            .toString());
        Assert.assertEquals("(TABLE1.NAME = 6 AND TABLE1.ID > 5)", ((JoinNode) nextJoin.getLeftNode()).getLeftNode()
            .getOtherJoinOnFilter()
            .toString());
        Assert.assertEquals("(TABLE2.ID < 10 AND TABLE2.NAME = 6)", ((JoinNode) nextJoin.getLeftNode()).getRightNode()
            .getOtherJoinOnFilter()
            .toString());

    }

    private void addOtherJoinFilter(JoinNode jn, String filter) {
        IFilter f = FilterUtils.createFilter(filter);
        List<List<IFilter>> DNFFilters = FilterUtils.toDNFNodesArray(FilterUtils.toDNFAndFlat(f));
        jn.setOtherJoinOnFilter(FilterUtils.DNFToAndLogicTree(DNFFilters.get(0)));
    }
}
