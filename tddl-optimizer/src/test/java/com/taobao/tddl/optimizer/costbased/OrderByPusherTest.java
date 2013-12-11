package com.taobao.tddl.optimizer.costbased;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.tddl.optimizer.BaseOptimizerTest;
import com.taobao.tddl.optimizer.core.ast.query.JoinNode;
import com.taobao.tddl.optimizer.core.ast.query.QueryNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.ast.query.strategy.IndexNestedLoopJoin;
import com.taobao.tddl.optimizer.costbased.pusher.OrderByPusher;

public class OrderByPusherTest extends BaseOptimizerTest {

    @Test
    public void test_order条件下推_子表_case1_下推NAME() {
        TableNode table1 = new TableNode("TABLE1");
        table1.alias("A");
        table1.orderBy("ID");

        QueryNode query = new QueryNode(table1);
        query.orderBy("A.ID");
        query.orderBy("A.NAME");
        query.build();

        OrderByPusher.optimize(query);

        Assert.assertEquals(2, table1.getOrderBys().size());
        Assert.assertEquals("TABLE1.ID", table1.getOrderBys().get(0).getColumn().toString());
        Assert.assertEquals("TABLE1.NAME", table1.getOrderBys().get(1).getColumn().toString());
    }

    @Test
    public void test_order条件下推_子表_case2_不下推() {
        TableNode table1 = new TableNode("TABLE1");
        table1.alias("A");
        table1.orderBy("ID");
        table1.orderBy("NAME");

        QueryNode query = new QueryNode(table1);
        query.orderBy("A.NAME");
        query.orderBy("A.SCHOOL");
        query.build();

        OrderByPusher.optimize(query);

        Assert.assertEquals(2, table1.getOrderBys().size());
        Assert.assertEquals("TABLE1.ID", table1.getOrderBys().get(0).getColumn().toString());
        Assert.assertEquals("TABLE1.NAME", table1.getOrderBys().get(1).getColumn().toString());
    }

    @Test
    public void test_order条件下推_子表_case3_下推IDNAME() {
        TableNode table1 = new TableNode("TABLE1");
        table1.alias("A");

        QueryNode query = new QueryNode(table1);
        query.orderBy("A.ID");
        query.orderBy("A.NAME");
        query.build();

        OrderByPusher.optimize(query);

        Assert.assertEquals(2, query.getOrderBys().size());
        Assert.assertEquals("TABLE1.ID", table1.getOrderBys().get(0).getColumn().toString());
        Assert.assertEquals("TABLE1.NAME", table1.getOrderBys().get(1).getColumn().toString());
    }

    @Test
    public void test_order条件下推_子表_case4_不下推函数() {
        TableNode table1 = new TableNode("TABLE1");
        table1.alias("A");

        QueryNode query = new QueryNode(table1);
        query.select("ID AS CID, (NAME+SCHOOL) AS NAME");
        query.orderBy("A.CID ");
        query.orderBy("A.NAME"); // 这里的name为select中的函数
        query.build();

        OrderByPusher.optimize(query);

        Assert.assertEquals(0, table1.getOrderBys().size());
    }

    @Test
    public void test_join条件下推_子表_case1_下推NAME() {
        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");
        table1.alias("A");
        table1.orderBy("ID");
        table2.alias("B");

        JoinNode join = table1.join(table2);
        join.setJoinStrategy(new IndexNestedLoopJoin());
        join.orderBy("A.ID");
        join.orderBy("A.NAME");
        join.build();

        OrderByPusher.optimize(join);

        Assert.assertEquals(2, table1.getOrderBys().size());
        Assert.assertEquals("TABLE1.ID", table1.getOrderBys().get(0).getColumn().toString());
        Assert.assertEquals("TABLE1.NAME", table1.getOrderBys().get(1).getColumn().toString());
    }

    @Test
    public void test_join条件下推_子表_case2_不下推() {
        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");
        table1.alias("A");
        table1.orderBy("ID");
        table1.orderBy("NAME");
        table2.alias("B");

        JoinNode join = table1.join(table2);
        join.setJoinStrategy(new IndexNestedLoopJoin());
        join.orderBy("A.NAME");
        join.orderBy("A.SCHOOL");
        join.build();

        OrderByPusher.optimize(join);

        Assert.assertEquals(2, table1.getOrderBys().size());
        Assert.assertEquals("TABLE1.ID", table1.getOrderBys().get(0).getColumn().toString());
        Assert.assertEquals("TABLE1.NAME", table1.getOrderBys().get(1).getColumn().toString());
    }

    @Test
    public void test_join条件下推_子表_case3_下推IDNAME() {
        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");
        table1.alias("A");
        table2.alias("B");

        JoinNode join = table1.join(table2);
        join.setJoinStrategy(new IndexNestedLoopJoin());
        join.orderBy("A.ID");
        join.orderBy("A.NAME");
        join.build();

        OrderByPusher.optimize(join);

        Assert.assertEquals(2, table1.getOrderBys().size());
        Assert.assertEquals("TABLE1.ID", table1.getOrderBys().get(0).getColumn().toString());
        Assert.assertEquals("TABLE1.NAME", table1.getOrderBys().get(1).getColumn().toString());
    }

    @Test
    public void test_join条件下推_子表_case4_函数不下推() {
        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");
        table1.alias("A");
        table2.alias("B");

        JoinNode join = table1.join(table2);
        join.setJoinStrategy(new IndexNestedLoopJoin());
        join.select("A.ID AS CID, (A.NAME + A.SCHOOL) AS NAME");
        join.orderBy("CID ");
        join.orderBy("NAME"); // 这里的name为select中的函数
        join.build();

        OrderByPusher.optimize(join);

        Assert.assertEquals(0, table1.getOrderBys().size());
    }
}
